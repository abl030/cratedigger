#!/usr/bin/env python3
"""Contract tests for web/routes/pipeline_dashboard.py.

Split from tests/web/test_routes_pipeline.py (#522), which itself split
from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

from datetime import datetime, timezone
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row


class TestPipelineDashboardRouteContracts(_FakeDbWebServerCase):
    """Contract tests for ``GET /api/pipeline/dashboard``."""

    DASHBOARD_REQUIRED_FIELDS = {
        "generated_at", "redis", "searches", "cycles", "coverage",
        "peers", "plan_readiness", "disk_coverage",
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

    def setUp(self) -> None:
        super().setUp()
        # The detail/log fixtures: one imported request with a track and
        # a real success download row, plus one wanted request.
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
            imported_path="/mnt/virtio/Music/Beets/Test",
        ))
        self.db.set_tracks(100, [
            {"disc_number": 1, "track_number": 1, "title": "Track",
             "length_seconds": 180},
        ])
        self.db.log_download(
            100, outcome="success", beets_scenario="strong_match",
            beets_distance=0.012, soulseek_username="testuser",
            filetype="mp3", bitrate=320000, actual_filetype="mp3",
            actual_min_bitrate=320, valid=True,
        )
        self.db.seed_request(make_request_row(
            id=101, status="wanted", source="request",
        ))

    def test_pipeline_dashboard_disk_coverage_contract(self):
        import web.server as srv

        self.db.seed_request(make_request_row(
            id=9101, status="imported", mb_release_id="dash-on-disk",
        ))
        self.db.seed_request(make_request_row(
            id=9102, status="imported", mb_release_id="dash-drifted",
            artist_name="Drift Artist", album_title="Drift Album",
        ))
        self.db.seed_request(make_request_row(
            id=9103, status="wanted", mb_release_id="dash-not-yet",
        ))
        self.db.seed_request(make_request_row(
            id=9104, status="downloading", mb_release_id="dash-in-flight",
        ))
        beets = FakeBeetsDB()
        beets.set_album_exists("dash-on-disk", True)
        # The class setUp baseline (id=100, imported) must read as
        # on-disk so it doesn't pollute the drift assertion below.
        beets.set_album_exists(self.db.request(100)["mb_release_id"], True)

        with patch.object(srv, "_beets_db", return_value=beets):
            status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        dc = data["disk_coverage"]
        _assert_required_fields(
            self, dc, {"counts", "drift_rows"},
            "pipeline dashboard disk coverage")
        _assert_required_fields(
            self, dc["counts"], self.DISK_COVERAGE_COUNT_FIELDS,
            "pipeline dashboard disk coverage counts")
        # Only off-disk `imported` rows are drift — wanted (not yet
        # acquired), downloading (in flight), and manual (staged for
        # review) are all expected to be absent from beets.
        self.assertEqual([r["id"] for r in dc["drift_rows"]], [9102])
        _assert_required_fields(
            self, dc["drift_rows"][0], self.DISK_COVERAGE_ROW_FIELDS,
            "pipeline dashboard drift row")

    def test_pipeline_dashboard_disk_coverage_null_without_beets(self):
        status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        self.assertIsNone(data["disk_coverage"])

    def _seed_dashboard_telemetry(self) -> None:
        """Real telemetry rows for every [0]-indexed dashboard assertion:
        cycle metrics (windows + wanted-trend samples), found/loop search
        logs (match-rate series, heavy queries, loop suspects), and peer
        observations (totals + days)."""
        from datetime import timedelta
        base = datetime.now(timezone.utc)
        self.db.record_cycle_metrics(
            cycle_total_s=300.0, browse_time_s=20.0, match_time_s=10.0,
            search_time_s=240.0, peers_browsed=8, fanout_waves=2,
            find_download_queued=4, find_download_completed=4,
            completed_at=base - timedelta(hours=2), wanted_total=12,
        )
        self.db.record_cycle_metrics(
            cycle_total_s=320.0, browse_time_s=22.0, match_time_s=11.0,
            search_time_s=250.0, peers_browsed=9, fanout_waves=3,
            find_download_queued=3, find_download_completed=3,
            completed_at=base - timedelta(minutes=5), wanted_total=10,
        )
        # One found search (match-rate series) + enough no_match rows on
        # the wanted request to register as a loop suspect, with browse
        # telemetry so the heavy-queries panel has a row.
        self.db.log_search(
            101, query="found query", outcome="found", result_count=5,
            elapsed_s=2.0, variant="v1", final_state="Completed",
            browse_time_s=42.0, match_time_s=1.0, peers_browsed=110,
            peers_browsed_lazy=5, fanout_waves=6,
        )
        for i in range(4):
            self.db.log_search(
                101, query=f"loop {i}", outcome="no_match",
                result_count=500, elapsed_s=12.0, variant="track_0",
                final_state="Completed", browse_time_s=42.0,
                match_time_s=1.0, peers_browsed=110, peers_browsed_lazy=5,
                fanout_waves=6,
            )
        self.db.record_peer_observations(["peer-a", "peer-b", "peer-c"])

    def test_pipeline_dashboard_contract(self):
        self._seed_dashboard_telemetry()
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


if __name__ == "__main__":
    unittest.main()
