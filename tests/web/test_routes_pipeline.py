#!/usr/bin/env python3
"""Contract tests for web/routes/pipeline.py read routes, triage, long-tail, beets-distance.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

import copy
from datetime import datetime, timezone
import os
import sys
import threading
import unittest
from unittest.mock import MagicMock, patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _MOCK_PIPELINE_REQUEST,
    _assert_required_fields,
    _WebServerCase,
    _fresh_triage_runner,
)

from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row


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
        "source", "youtube_metadata",
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
    WRONG_MATCH_TRIAGE_SUMMARY_REQUIRED_FIELDS = {
        "processed", "deleted", "deleted_verified_lossless_parent",
        "kept_would_import", "kept_uncertain",
        "skipped_candidate_evidence_missing", "skipped_candidate_evidence_stale",
        "skipped_current_evidence_missing", "skipped_current_evidence_stale",
        "skipped_current_evidence_failed",
        "skipped_active_job", "skipped_invalid_row", "skipped_missing_path",
        "skipped_operational", "delete_failed", "results",
    }
    WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS = {
        "state", "started_at", "finished_at", "summary", "error",
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
        "peers", "plan_readiness",
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
    DASHBOARD_PEERS_FIELDS = {
        "totals", "days", "heavy_queries", "heavy_query_hours",
    }
    DASHBOARD_PEERS_TOTAL_FIELDS = {
        "known_peers", "new_24h", "seen_24h", "tracked_since",
    }
    DASHBOARD_PEERS_DAY_FIELDS = {
        "date", "new_peers", "total_peers",
    }
    DASHBOARD_PEER_BROWSE_HEAVY_QUERY_FIELDS = {
        "search_log_id", "request_id", "mb_release_id", "artist_name",
        "album_title", "status", "created_at", "query", "variant",
        "outcome", "result_count", "elapsed_s", "browse_time_s",
        "match_time_s", "peers_browsed", "peers_browsed_lazy",
        "peer_dirs", "fanout_waves",
    }
    DISK_COVERAGE_COUNT_FIELDS = {
        "active_total", "on_disk_total", "off_disk_total", "by_status",
        "on_disk_by_status", "off_disk_by_status", "inverse_total",
    }
    DISK_COVERAGE_ROW_FIELDS = {
        "id", "status", "artist_name", "album_title", "mb_release_id",
        "discogs_release_id",
    }
    DISK_COVERAGE_INVERSE_FIELDS = {
        "id", "album", "albumartist", "mb_albumid", "discogs_albumid",
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
        self.mock_db.get_latest_download_summaries.return_value = {}
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

    def test_disk_coverage_contract(self):
        from tests.fakes import FakeBeetsDB
        import web.server as srv

        fake = self.mock_db._fake
        fake.seed_request(make_request_row(
            id=9001, status="wanted", mb_release_id="disk-missing-mbid",
            artist_name="Missing Artist", album_title="Missing Album",
        ))
        beets = FakeBeetsDB()

        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/disk-coverage")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, {"counts", "off_disk", "inverse"},
            "disk coverage response")
        _assert_required_fields(
            self, data["counts"], self.DISK_COVERAGE_COUNT_FIELDS,
            "disk coverage counts")
        _assert_required_fields(
            self, data["off_disk"][0], self.DISK_COVERAGE_ROW_FIELDS,
            "disk coverage off-disk row")

    def test_disk_coverage_inverse_contract(self):
        from tests.fakes import FakeBeetsDB
        import web.server as srv

        beets = FakeBeetsDB()
        beets.set_release_identities([
            {
                "id": 77,
                "album": "Untracked Album",
                "albumartist": "Untracked Artist",
                "mb_albumid": "beets-only-mbid",
                "discogs_albumid": None,
            },
        ])

        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/disk-coverage?inverse=1")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data["inverse"][0], self.DISK_COVERAGE_INVERSE_FIELDS,
            "disk coverage inverse row")

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
        self.mock_db.get_by_status.side_effect = (
            lambda s, **kw: [row] if s == "wanted" else [])

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"counts", "wanted", "downloading", "imported", "manual",
                                             "imported_total", "imported_truncated"},
                                "pipeline all response")
        _assert_required_fields(self, data["wanted"][0], self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline all item")

    def test_pipeline_all_imported_is_a_recency_window(self):
        """#426: the imported bucket is capped (newest first) and the
        payload flags the truncation so the UI can say so."""
        from web.routes.pipeline import IMPORTED_RECENT_LIMIT
        row = make_request_row(id=301, status="imported",
                               album_title="Imported Album")
        calls = []

        def _by_status(s, **kw):
            calls.append((s, kw))
            return [row] if s == "imported" else []

        self.mock_db.get_by_status.side_effect = _by_status
        self.mock_db.count_by_status.return_value = {
            "wanted": 0, "imported": IMPORTED_RECENT_LIMIT + 50, "manual": 0,
        }

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        self.assertEqual(data["imported_total"], IMPORTED_RECENT_LIMIT + 50)
        self.assertTrue(data["imported_truncated"])
        imported_call = next(c for c in calls if c[0] == "imported")
        self.assertEqual(imported_call[1],
                         {"limit": IMPORTED_RECENT_LIMIT, "newest_first": True})

    SEARCH_REQUIRED_FIELDS = {"query", "items", "total"}

    def test_pipeline_search_contract(self):
        row = make_request_row(id=401, status="imported",
                               artist_name="The Mountain Goats",
                               album_title="Tallahassee")
        self.mock_db.search_requests.return_value = [row]

        status, data = self._get("/api/pipeline/search?q=mountain")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.SEARCH_REQUIRED_FIELDS,
                                "pipeline search response")
        self.assertEqual(data["query"], "mountain")
        self.assertEqual(data["total"], 1)
        _assert_required_fields(self, data["items"][0],
                                self.PIPELINE_ITEM_REQUIRED_FIELDS,
                                "pipeline search item")

    def test_pipeline_search_blank_query_is_empty(self):
        self.mock_db.search_requests.return_value = []
        status, data = self._get("/api/pipeline/search")
        self.assertEqual(status, 200)
        self.assertEqual(data["items"], [])

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
        _assert_required_fields(self, data["peers"],
                                self.DASHBOARD_PEERS_FIELDS,
                                "pipeline dashboard peers")
        _assert_required_fields(self, data["peers"]["totals"],
                                self.DASHBOARD_PEERS_TOTAL_FIELDS,
                                "pipeline dashboard peer totals")
        _assert_required_fields(self, data["peers"]["days"][0],
                                self.DASHBOARD_PEERS_DAY_FIELDS,
                                "pipeline dashboard peer day")
        _assert_required_fields(self, data["peers"]["heavy_queries"][0],
                                self.DASHBOARD_PEER_BROWSE_HEAVY_QUERY_FIELDS,
                                "pipeline dashboard peer browse heavy query")
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
        full slice (up to 20) by (matched_tracks DESC, avg_ratio DESC) via
        msgspec.to_builtins."""
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
        # All 4 (≤20 cap), sorted by (matched_tracks DESC, avg_ratio DESC):
        # u1 (26, 0.95) → u3 (26, 0.85) → u2 (22, 0.80) → u4 (20, 0.99)
        usernames = [c["username"] for c in last["top_candidates"]]
        self.assertEqual(usernames, ["u1", "u3", "u2", "u4"])
        for cand in last["top_candidates"]:
            _assert_required_fields(self, cand,
                                    self.CANDIDATE_SCORE_REQUIRED_FIELDS,
                                    "candidate score")

    def test_pipeline_detail_caps_top_candidates_at_twenty(self):
        """U2: the peers panel widened from 3 to the full stored cap (20). A
        search row with >20 candidates surfaces exactly 20, still ranked."""
        blob = [
            {"username": f"u{i:02d}", "dir": f"D{i}", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26,
             "avg_ratio": 1.0 - i / 100.0,
             "missing_titles": [], "file_count": 26}
            for i in range(25)
        ]
        self.mock_db.get_search_history.return_value = [{
            "id": 99, "request_id": 100, "query": "q",
            "result_count": 100, "elapsed_s": 1.0, "outcome": "no_match",
            "created_at": "2026-04-29T00:00:00+00:00",
            "candidates": blob,
            "variant": "v3_artist_only", "final_state": "Completed",
        }]
        try:
            status, data = self._get("/api/pipeline/100")
        finally:
            self.mock_db.get_search_history.return_value = []
        self.assertEqual(status, 200)
        top = data["last_search"]["top_candidates"]
        self.assertEqual(len(top), 20)
        # All matched_tracks equal → highest avg_ratio first: u00..u19
        self.assertEqual(top[0]["username"], "u00")
        self.assertEqual(top[-1]["username"], "u19")

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
    def test_wrong_match_triage_starts_background_sweep(self, mock_cleanup):
        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        runner = _fresh_triage_runner(self)
        mock_cleanup.return_value = WrongMatchCleanupSummary(
            processed=2,
            deleted=1,
            kept_uncertain=1,
        )
        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })

        # Issue: bulk triage must not hold the single server thread — the
        # POST returns immediately and the sweep runs on a background thread.
        self.assertEqual(status, 202)
        self.assertEqual(data["status"], "started")
        self.assertEqual(data["state"], "running")

        runner.join(timeout=5)
        mock_cleanup.assert_called_once_with(
            self.mock_db,
            confirm_all_wrong_matches=True,
        )

        status, data = self._get("/api/wrong-matches/triage/status")
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS,
            "wrong match triage status response")
        self.assertEqual(data["state"], "completed")
        self.assertIsNone(data["error"])
        _assert_required_fields(
            self, data["summary"],
            self.WRONG_MATCH_TRIAGE_SUMMARY_REQUIRED_FIELDS,
            "wrong match triage summary")
        self.assertEqual(data["summary"]["processed"], 2)
        self.assertEqual(data["summary"]["deleted"], 1)

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_rejects_concurrent_sweep(self, mock_cleanup):
        import threading

        from lib.wrong_match_cleanup_service import WrongMatchCleanupSummary
        runner = _fresh_triage_runner(self)
        release = threading.Event()
        entered = threading.Event()

        def slow_cleanup(db, *, confirm_all_wrong_matches):
            entered.set()
            release.wait(timeout=5)
            return WrongMatchCleanupSummary(processed=0)

        mock_cleanup.side_effect = slow_cleanup

        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })
        self.assertEqual(status, 202)
        self.assertTrue(entered.wait(timeout=5))

        status, data = self._post("/api/wrong-matches/triage", {
            "confirm_all_wrong_matches": True,
        })
        self.assertEqual(status, 409)
        self.assertIn("already running", data["error"])

        status, data = self._get("/api/wrong-matches/triage/status")
        self.assertEqual(status, 200)
        self.assertEqual(data["state"], "running")
        self.assertIsNone(data["summary"])

        release.set()
        runner.join(timeout=5)

    def test_wrong_match_triage_status_idle_contract(self):
        _fresh_triage_runner(self)
        status, data = self._get("/api/wrong-matches/triage/status")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, self.WRONG_MATCH_TRIAGE_STATUS_REQUIRED_FIELDS,
            "wrong match triage status response")
        self.assertEqual(data["state"], "idle")
        self.assertIsNone(data["summary"])
        self.assertIsNone(data["error"])

    @patch("web.routes.imports.cleanup_all_wrong_matches")
    def test_wrong_match_triage_requires_full_queue_confirmation(self, mock_cleanup):
        _fresh_triage_runner(self)
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

    ``tests/web/_harness.py`` puts ``lib/`` on sys.path, reproducing the
    same PYTHONPATH ambiguity production has. A future regression of the
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


class TestLongTailRouteContracts(_WebServerCase):
    """U1 contract for ``GET /api/pipeline/long-tail``.

    Wraps ``lib.long_tail_service.list_long_tail`` — the same service
    ``pipeline-cli long-tail`` wraps (CLI ⇄ API symmetry). Drives the
    real service + DB cohort query against a fresh :class:`FakePipelineDB`
    (no service mocking, per MOCKS: LEAF-SEAM ONLY). Banding's beets
    collaborators (``check_beets_library`` / ``_beets_db`` /
    ``compute_library_rank``) are the leaf seam — patched at
    ``web.server`` only when a test exercises an in-library band.
    """

    # The frontend long-tail list renders these fields per row out of the
    # serialized ``LongTailRow``. Pin every one so a rename can't silently
    # break the JS.
    ROW_REQUIRED_FIELDS = {
        "id", "artist_name", "album_title", "year", "status", "source",
        "mb_release_id", "discogs_release_id", "target_format",
        "min_bitrate", "search_filetype_override", "unfindable_category",
        "band", "in_flight_rescue",
        # Card meta (year · MB/Discogs · N tracks) + on-disk spectral strip.
        "track_count", "current_spectral_grade", "current_spectral_bitrate",
    }
    ENVELOPE_REQUIRED_FIELDS = {"results", "band", "count"}

    _LONG_TAIL_DB_METHODS = (
        "get_long_tail_cohort",
        "get_long_tail_request",
    )

    def setUp(self) -> None:
        fresh = FakePipelineDB()
        self._old_backing = self.mock_db._fake
        self.mock_db._mock_wraps = fresh
        self.mock_db._fake = fresh
        self._lt_method_state: dict[str, MagicMock] = {}
        for name in self._LONG_TAIL_DB_METHODS:
            self._lt_method_state[name] = getattr(self.mock_db, name)
            forwarder = MagicMock(side_effect=getattr(fresh, name))
            setattr(self.mock_db, name, forwarder)

    def tearDown(self) -> None:
        for name, prev in self._lt_method_state.items():
            setattr(self.mock_db, name, prev)
        self.mock_db._mock_wraps = self._old_backing
        self.mock_db._fake = self._old_backing

    @property
    def _fake(self) -> "FakePipelineDB":
        return self.mock_db._fake

    def test_missing_row_bands_missing_and_imported_absent(self):
        """AE1 at the HTTP boundary: a wanted row with no beets album
        bands ``missing``; an imported request is absent from the
        result. (No beets configured → everything Missing.)"""
        from lib.long_tail_service import LongTailRow
        self._fake.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1",
            artist_name="Vanishing", album_title="Lost"))
        self._fake.seed_request(make_request_row(
            id=2, status="imported", mb_release_id="rel-2"))

        status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, self.ENVELOPE_REQUIRED_FIELDS,
                                "long-tail envelope")
        self.assertEqual(data["count"], 1)
        self.assertIsNone(data["band"])
        row = data["results"][0]
        _assert_required_fields(self, row, self.ROW_REQUIRED_FIELDS,
                                "long-tail row")
        self.assertEqual(row["id"], 1)
        self.assertEqual(row["band"], "missing")
        self.assertFalse(row["in_flight_rescue"])
        # Wire shape IS the Struct shape — round-trips cleanly.
        back = msgspec.convert(row, type=LongTailRow)
        self.assertEqual(back.id, 1)

    def test_transparent_band_via_beets_seam(self):
        """AE2 at the HTTP boundary: a wanted row whose beets copy
        classifies Transparent bands ``transparent``. The beets leaf
        seam is patched to report the release in-library with a
        lossless detail row."""
        import web.server as srv
        self._fake.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))

        mock_beets = MagicMock()
        # MP3 @ 256 kbps classifies TRANSPARENT in the default rank model
        # (Opus 128 / MP3 V0 are transparent; see docs/quality-ranks.md).
        mock_beets.check_mbids_detail.return_value = {
            "rel-1": {"beets_format": "MP3", "beets_bitrate": 256},
        }
        with patch("web.server.check_beets_library",
                   return_value={"rel-1"}), \
                patch("web.server._beets_db", return_value=mock_beets):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "transparent")

    def test_unknown_band_when_in_library_but_unrankable(self):
        """In-library but no detail / unrankable → ``unknown``, not
        ``missing``."""
        self._fake.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))

        mock_beets = MagicMock()
        mock_beets.check_mbids_detail.return_value = {}  # no detail row
        with patch("web.server.check_beets_library",
                   return_value={"rel-1"}), \
                patch("web.server._beets_db", return_value=mock_beets):
            status, data = self._get("/api/pipeline/long-tail")

        self.assertEqual(status, 200)
        self.assertEqual(data["results"][0]["band"], "unknown")

    def test_in_flight_rescue_stamped(self):
        self._fake.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        self._fake.insert_youtube_running(
            request_id=1, browse_id="MPREb_z", audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=z",
            expected_track_count=10,
        )
        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        self.assertTrue(data["results"][0]["in_flight_rescue"])

    def test_band_filter_narrows_result(self):
        self._fake.seed_request(make_request_row(
            id=1, status="wanted", mb_release_id="rel-1"))
        self._fake.seed_request(make_request_row(
            id=2, status="wanted", mb_release_id="rel-2"))
        # No beets → both Missing.
        status, data = self._get("/api/pipeline/long-tail?band=missing")
        self.assertEqual(status, 200)
        self.assertEqual(data["band"], "missing")
        self.assertEqual({r["id"] for r in data["results"]}, {1, 2})
        # A band with no members returns an empty cohort, still 200.
        status, data = self._get("/api/pipeline/long-tail?band=transparent")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)

    def test_empty_cohort_returns_200(self):
        status, data = self._get("/api/pipeline/long-tail")
        self.assertEqual(status, 200)
        self.assertEqual(data["count"], 0)
        self.assertEqual(data["results"], [])

    def test_single_id_returns_one_banded_row(self):
        """KTD8: ``?id=`` returns just that request's authoritative band."""
        from lib.long_tail_service import LongTailRow
        self._fake.seed_request(make_request_row(
            id=42, status="wanted", mb_release_id="rel-42",
            artist_name="One", album_title="Row"))
        status, data = self._get("/api/pipeline/long-tail?id=42")
        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"result", "id"},
                                "long-tail single-id envelope")
        self.assertEqual(data["id"], 42)
        row = msgspec.convert(data["result"], type=LongTailRow)
        self.assertEqual(row.id, 42)
        self.assertEqual(row.band, "missing")

    def test_single_id_404_when_not_wanted(self):
        self._fake.seed_request(make_request_row(
            id=42, status="imported", mb_release_id="rel-42"))
        status, data = self._get("/api/pipeline/long-tail?id=42")
        self.assertEqual(status, 404)
        self.assertEqual(data["id"], 42)

    def test_single_id_400_on_non_int(self):
        status, data = self._get("/api/pipeline/long-tail?id=not-an-int")
        self.assertEqual(status, 400)
        self.assertIn("error", data)

if __name__ == "__main__":
    unittest.main()
