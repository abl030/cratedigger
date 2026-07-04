#!/usr/bin/env python3
"""Contract tests for web/routes/pipeline.py read routes + beets-distance.

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py. Triage and long-tail contract tests moved to
tests/web/test_routes_triage.py / tests/web/test_routes_long_tail.py
(#481 item 3), following web/routes/pipeline.py's own split.
"""

from datetime import datetime, timezone
import os
import sys
import threading
import unittest
from unittest.mock import patch

import msgspec

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import (
    _assert_required_fields,
    _FakeDbWebServerCase,
    _fresh_triage_runner,
)

from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row


class TestPipelineRouteContracts(_FakeDbWebServerCase):
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
    DISK_COVERAGE_INVERSE_FIELDS = {
        "id", "album", "albumartist", "mb_albumid", "discogs_albumid",
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
        import web.server as srv

        self.db.seed_request(make_request_row(
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

    def test_pipeline_log_surfaces_wrong_match_triage_audit(self):
        self.db.log_download(
            100, outcome="rejected", beets_scenario="high_distance",
            beets_distance=0.190, soulseek_username="moundsofass",
            validation_result={
                "scenario": "wrong_match",
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["mp3_spectral:reject"],
                },
            },
        )

        status, data = self._get("/api/pipeline/log")

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
        self.db.seed_request(make_request_row(
            id=201, status="wanted", album_title="Wanted Album"))

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
        from datetime import timedelta
        from web.routes.pipeline import IMPORTED_RECENT_LIMIT
        # setUp already seeded one imported row (id=100); add enough to
        # exceed the cap by 10. Stagger updated_at so newest-first
        # ordering is observable.
        base = datetime(2026, 5, 1, tzinfo=timezone.utc)
        for i in range(IMPORTED_RECENT_LIMIT + 10):
            self.db.seed_request(make_request_row(
                id=1000 + i, status="imported",
                album_title=f"Imported {i}",
                updated_at=base + timedelta(minutes=i),
            ))

        status, data = self._get("/api/pipeline/all")

        self.assertEqual(status, 200)
        self.assertEqual(data["imported_total"], IMPORTED_RECENT_LIMIT + 11)
        self.assertTrue(data["imported_truncated"])
        # The bucket is capped at the limit, newest first.
        self.assertEqual(len(data["imported"]), IMPORTED_RECENT_LIMIT)
        self.assertEqual(data["imported"][0]["album_title"],
                         f"Imported {IMPORTED_RECENT_LIMIT + 9}")

    SEARCH_REQUIRED_FIELDS = {"query", "items", "total"}

    def test_pipeline_search_contract(self):
        self.db.seed_request(make_request_row(
            id=401, status="imported",
            artist_name="The Mountain Goats",
            album_title="Tallahassee"))

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
        status, data = self._get("/api/pipeline/search")
        self.assertEqual(status, 200)
        self.assertEqual(data["items"], [])

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
        from lib.quality import CandidateScore
        candidates_blob = msgspec.convert([
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
        ], type=list[CandidateScore])
        self.db.log_search(
            100, query="*rtist Album", result_count=100, elapsed_s=1.2,
            outcome="no_match", candidates=candidates_blob,
            variant="v3_artist_only", final_state="Completed",
        )

        status, data = self._get("/api/pipeline/100")

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
        from lib.quality import CandidateScore
        blob = msgspec.convert([
            {"username": f"u{i:02d}", "dir": f"D{i}", "filetype": "flac",
             "matched_tracks": 26, "total_tracks": 26,
             "avg_ratio": 1.0 - i / 100.0,
             "missing_titles": [], "file_count": 26}
            for i in range(25)
        ], type=list[CandidateScore])
        self.db.log_search(
            100, query="q", result_count=100, elapsed_s=1.0,
            outcome="no_match", candidates=blob,
            variant="v3_artist_only", final_state="Completed",
        )
        status, data = self._get("/api/pipeline/100")
        self.assertEqual(status, 200)
        top = data["last_search"]["top_candidates"]
        self.assertEqual(len(top), 20)
        # All matched_tracks equal → highest avg_ratio first: u00..u19
        self.assertEqual(top[0]["username"], "u00")
        self.assertEqual(top[-1]["username"], "u19")

    def test_pipeline_detail_handles_null_candidates_gracefully(self):
        """Historical search_log row with NULL candidates → top_candidates=[]."""
        self.db.log_search(
            100, query="q", result_count=None, elapsed_s=None,
            outcome="timeout", candidates=None,
            variant=None, final_state=None,
        )
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertIsNone(data["last_search"]["variant"])

    def test_pipeline_detail_handles_empty_candidates_list(self):
        """Latest search row with an empty candidates list → top_candidates=[]."""
        self.db.log_search(
            100, query="q", result_count=0, elapsed_s=0.5,
            outcome="no_results", candidates=[],
            variant="v2_artist_album_no_year", final_state="Completed",
        )
        status, data = self._get("/api/pipeline/100")

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
        import json as _json
        self.db.log_search(
            100, query="q", result_count=5, elapsed_s=0.5,
            outcome="no_match", candidates=[],
            variant="v2_artist_album_no_year", final_state="Completed",
        )
        # Corrupt the stored JSONB in place — historical rows whose
        # shape predates CandidateScore. The fake stores the encoded
        # JSON string exactly like the real column.
        self.db.search_logs[-1].candidates = _json.dumps([{"foo": "bar"}])
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertIsNotNone(data["last_search"])
        self.assertEqual(data["last_search"]["top_candidates"], [])
        self.assertEqual(data["last_search"]["variant"],
                         "v2_artist_album_no_year")

    def test_pipeline_detail_surfaces_manual_reason(self):
        """manual_reason='search_exhausted' is exposed on the detail response."""
        self.db.update_request_fields(
            100, status="manual", manual_reason="search_exhausted")
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        self.assertEqual(data["manual_reason"], "search_exhausted")

    def test_pipeline_detail_history_surfaces_wrong_match_triage_audit(self):
        self.db.log_download(
            100, outcome="rejected", beets_scenario="high_distance",
            beets_distance=0.190,
            validation_result={
                "wrong_match_triage": {
                    "action": "deleted_reject",
                    "reason": "requeue_upgrade",
                    "preview_verdict": "confident_reject",
                    "preview_decision": "requeue_upgrade",
                    "stage_chain": ["stage1_spectral:reject"],
                },
            },
        )
        status, data = self._get("/api/pipeline/100")

        self.assertEqual(status, 200)
        item = data["history"][0]
        self.assertEqual(item["wrong_match_triage_action"], "deleted_reject")
        self.assertIn("spectral", item["wrong_match_triage_summary"])
        self.assertEqual(item["wrong_match_triage_preview_verdict"],
                         "confident_reject")
        self.assertEqual(item["wrong_match_triage_stage_chain"],
                         ["stage1_spectral:reject"])

    def test_pipeline_recent_contract(self):
        self.db.seed_request(make_request_row(
            id=202, status="imported", album_title="Recent Album"))
        self.db.set_tracks(202, [
            {"disc_number": 1, "track_number": n, "title": f"T{n}"}
            for n in range(1, 12)
        ])
        self.db.log_download(
            202, outcome="success", beets_scenario="strong_match",
            beets_distance=0.01, soulseek_username="testuser",
        )

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
        self.db.seed_request(make_request_row(
            id=303, status="imported", album_title="Recent Album",
            mb_release_id="no-such-id-in-beets"))
        self.db.set_tracks(303, [
            {"disc_number": 1, "track_number": n, "title": f"T{n}"}
            for n in range(1, 9)
        ])

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
            self.db,
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

    def _enqueue_force_job(self) -> int:
        from lib.import_queue import force_import_dedupe_key
        log_id = self.db.log_download(
            100, outcome="rejected", soulseek_username="baduser",
            validation_result={"failed_path": "/tmp/Test Album"},
        )
        job = self.db.enqueue_import_job(
            "force_import", request_id=100,
            dedupe_key=force_import_dedupe_key(log_id),
            payload={"failed_path": "/tmp/Test Album"},
            message="Import queued",
        )
        return job.id

    def test_import_jobs_contract(self):
        self._enqueue_force_job()
        status, data = self._get("/api/import-jobs")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"}, "import jobs response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs item")

    def test_import_job_detail_contract(self):
        job_id = self._enqueue_force_job()
        status, data = self._get(f"/api/import-jobs/{job_id}")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"job"}, "import job detail response")
        _assert_required_fields(self, data["job"], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import job detail")

    def test_import_jobs_timeline_contract(self):
        self._enqueue_force_job()
        status, data = self._get("/api/import-jobs/timeline")

        self.assertEqual(status, 200)
        _assert_required_fields(self, data, {"jobs", "counts"},
                                "import jobs timeline response")
        _assert_required_fields(self, data["jobs"][0], self.IMPORT_JOB_REQUIRED_FIELDS,
                                "import jobs timeline item")
        _assert_required_fields(self, data["jobs"][0], {"artist_name", "album_title"},
                                "import jobs timeline identity")
        # The identity join resolved through the seeded request row.
        self.assertEqual(data["jobs"][0]["artist_name"],
                         self.db.request(100)["artist_name"])

    def test_import_jobs_timeline_caps_at_50(self):
        """The route hardcodes limit=50 — seed 51 jobs, count the page."""
        for i in range(51):
            self.db.enqueue_import_job(
                "force_import", request_id=100,
                dedupe_key=f"force_import:download_log:{i}",
                payload={"failed_path": f"/tmp/a{i}"},
            )
        status, data = self._get("/api/import-jobs/timeline")
        self.assertEqual(status, 200)
        self.assertEqual(len(data["jobs"]), 50)

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


class TestPipelineRouteDirectEquivalence(_FakeDbWebServerCase):
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

    The dual-load ambiguity itself is gone (#445 item 3: one canonical
    import name per module, enforced by tests/test_no_dual_load.py and
    TestSysPathAudit), but the equivalence contract this test pins is
    independent of how a divergence arises — keep it.
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


class TestBeetsDistanceRouteContract(_FakeDbWebServerCase):
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
        super().setUp()
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


if __name__ == "__main__":
    unittest.main()
