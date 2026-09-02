"""Contract tests for web/routes/pipeline_dashboard.py.

Split from tests/web/test_routes_pipeline.py (#522), which itself split
from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""
import os
import sys
import tempfile
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from lib.beets_db import BeetsAlbumIdentityRow
from lib.cycle_counters import CycleCounters
from lib.library_completeness import (
    CompletenessAlbum,
    CompletenessCounts,
    CompletenessFinding,
    CompletenessReport,
)
from lib.library_completeness_snapshot import (
    LibraryCompletenessSnapshot,
    library_completeness_snapshot_path,
    library_completeness_trigger_path,
    read_library_completeness_snapshot,
    write_library_completeness_snapshot,
)
from lib.retag_divergence_audit import (
    RetagDivergenceAlbum,
    RetagDivergenceCounts,
    RetagDivergenceReport,
    RetagDivergenceStatus,
)
from lib.retag_divergence_census_snapshot import (
    RetagDivergenceCensusSnapshot,
    read_retag_divergence_census_snapshot,
    retag_divergence_census_snapshot_path,
    write_retag_divergence_census_snapshot,
)
from tests.fakes import FakeBeetsDB
from tests.helpers import make_request_row, make_web_runtime
from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase
from web.runtime import install_runtime, runtime


class TestPipelineDashboardRouteContracts(_FakeDbWebServerCase):
    """Contract tests for ``GET /api/pipeline/dashboard``."""

    DASHBOARD_REQUIRED_FIELDS: ClassVar = {
        "generated_at", "redis", "searches", "cycles", "coverage",
        "peers", "plan_readiness", "disk_coverage", "unfindable",
        "retag_divergence_census",
        "library_completeness",
    }
    DASHBOARD_UNFINDABLE_FIELDS: ClassVar = {
        "recent_runs", "backlog_trend",
    }
    DASHBOARD_UNFINDABLE_RUN_FIELDS: ClassVar = {
        "id", "created_at", "cohort_total", "due_backlog_at_start",
        "batch_limit", "candidates_processed", "probes_attempted",
        "categorised_count", "downgraded_count", "no_change_count",
        "probe_failed_count", "not_due_count", "request_not_found_count",
        "breaker_tripped", "duration_seconds",
    }
    DASHBOARD_UNFINDABLE_BACKLOG_TREND_FIELDS: ClassVar = {
        "current_backlog", "latest_sample_at", "series",
    }
    DASHBOARD_UNFINDABLE_BACKLOG_TREND_POINT_FIELDS: ClassVar = {
        "sampled_at", "due_backlog_at_start", "candidates_processed",
    }
    DASHBOARD_SEARCH_WINDOW_FIELDS: ClassVar = {
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
    DASHBOARD_PLAN_READINESS_FIELDS: ClassVar = {
        "generator_id", "wanted_total", "wanted_searchable",
        "wanted_legacy", "wanted_failed_deterministic",
        "wanted_failed_transient", "wanted_no_plan",
    }
    DASHBOARD_CYCLE_WINDOW_FIELDS: ClassVar = {
        "label", "hours", "cycles", "avg_cycle_s", "median_cycle_s",
        "p95_cycle_s", "max_cycle_s", "median_search_s", "watchdog_kills",
        "find_download_queued", "find_download_completed", "cache_errors",
        "cache_write_errors", "cache_fuse_tripped", "peers_browsed",
        "peers_browsed_lazy", "fanout_waves",
    }
    DASHBOARD_COVERAGE_FIELDS: ClassVar = {
        "wanted_total", "wanted_searched_24h", "wanted_searched_6h",
        "wanted_unsearched_24h", "wanted_unsearched_6h",
        "wanted_never_searched", "active_wanted_searches_24h",
        "active_wanted_searches_6h", "oldest_last_search_at",
        "matches_24h", "matches_6h", "matches_per_hour_24h",
        "matches_per_hour_6h", "match_rate_series_24h",
        "match_rate_series_28d", "wanted_trend", "top_10_share_24h",
        "top_loop_suspects", "stale_wanted",
    }
    DASHBOARD_WANTED_TREND_FIELDS: ClassVar = {
        "current_wanted", "latest_sample_at", "series_24h", "windows",
    }
    DASHBOARD_WANTED_TREND_POINT_FIELDS: ClassVar = {
        "sampled_at", "wanted_total",
    }
    DASHBOARD_WANTED_TREND_WINDOW_FIELDS: ClassVar = {
        "label", "hours", "sample_count", "start_sample_at",
        "end_sample_at", "start_wanted", "end_wanted", "delta",
        "delta_per_hour", "drain_per_hour", "eta_hours", "trend",
    }
    DASHBOARD_MATCH_RATE_POINT_FIELDS: ClassVar = {
        "bucket_start", "matches", "matches_per_hour",
    }
    DASHBOARD_DAILY_MATCH_RATE_POINT_FIELDS: ClassVar = {
        "bucket_start", "matches", "matches_per_day",
    }
    DASHBOARD_PEERS_FIELDS: ClassVar = {
        "totals", "days", "heavy_queries", "heavy_query_hours",
    }
    DASHBOARD_PEERS_TOTAL_FIELDS: ClassVar = {
        "known_peers", "new_24h", "seen_24h", "tracked_since",
    }
    DASHBOARD_PEERS_DAY_FIELDS: ClassVar = {
        "date", "new_peers", "total_peers",
    }
    DASHBOARD_PEER_BROWSE_HEAVY_QUERY_FIELDS: ClassVar = {
        "search_log_id", "request_id", "mb_release_id", "artist_name",
        "album_title", "status", "created_at", "query", "variant",
        "outcome", "result_count", "elapsed_s", "browse_time_s",
        "match_time_s", "peers_browsed", "peers_browsed_lazy",
        "peer_dirs", "fanout_waves",
    }
    DISK_COVERAGE_COUNT_FIELDS: ClassVar = {
        "active_total", "on_disk_total", "off_disk_total", "by_status",
        "on_disk_by_status", "off_disk_by_status", "inverse_total",
    }
    DISK_COVERAGE_ROW_FIELDS: ClassVar = {
        "id", "status", "artist_name", "album_title", "mb_release_id",
        "discogs_release_id", "source", "resolution",
    }

    def setUp(self) -> None:
        super().setUp()
        # The detail/log fixtures: one imported request with a track and
        # a real success download row, plus one wanted request.
        self.db.seed_request(make_request_row(
            id=100, status="imported", min_bitrate=320,
            mb_release_id="00000000-0000-4000-8000-000000000100",
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
            mb_release_id="00000000-0000-4000-8000-000000000101",
        ))

    def test_pipeline_dashboard_disk_coverage_contract(self):
        self.db.seed_request(make_request_row(
            id=9101, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000009101",
        ))
        self.db.seed_request(make_request_row(
            id=9102, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000009102",
            artist_name="Drift Artist", album_title="Drift Album",
        ))
        self.db.seed_request(make_request_row(
            id=9103, status="wanted",
            mb_release_id="00000000-0000-4000-8000-000000009103",
        ))
        self.db.seed_request(make_request_row(
            id=9104, status="downloading",
            mb_release_id="00000000-0000-4000-8000-000000009104",
        ))
        self.db.seed_request(make_request_row(
            id=9105, status="unsearchable",
            mb_release_id="00000000-0000-4000-8000-000000009105",
        ))
        self.db.seed_request(make_request_row(
            id=9106, status="imported",
            mb_release_id="00000000-0000-4000-8000-000000009106",
        ))
        beets = FakeBeetsDB()
        beets.set_album_exists("00000000-0000-4000-8000-000000009101", True)
        beets.set_album_ids_for_release(
            "00000000-0000-4000-8000-000000009106", [71, 72],
        )
        # The class setUp baseline (id=100, imported) must read as
        # on-disk so it doesn't pollute the drift assertion below.
        beets.set_album_exists(self.db.request(100)["mb_release_id"], True)

        with install_runtime(make_web_runtime(runtime(), beets=beets)):
            status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        dc = data["disk_coverage"]
        _assert_required_fields(
            self, dc, {"counts", "drift_rows"},
            "pipeline dashboard disk coverage")
        _assert_required_fields(
            self, dc["counts"], self.DISK_COVERAGE_COUNT_FIELDS,
            "pipeline dashboard disk coverage counts")
        # Only imported rows that are not uniquely resolvable in Beets are
        # drift — wanted, downloading, and unsearchable lifecycle rows can
        # legitimately be missing or ambiguous.
        self.assertEqual([r["id"] for r in dc["drift_rows"]], [9102, 9106])
        _assert_required_fields(
            self, dc["drift_rows"][0], self.DISK_COVERAGE_ROW_FIELDS,
            "pipeline dashboard drift row")
        self.assertEqual(dc["drift_rows"][0]["resolution"], {
            "kind": "missing",
        })
        self.assertEqual(dc["drift_rows"][1]["resolution"], {
            "kind": "ambiguous", "album_ids": [71, 72],
            "reason": "multiple_matches",
        })

    def test_pipeline_dashboard_disk_coverage_null_without_beets(self):
        with install_runtime(replace(runtime(), shared_beets=None)):
            status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        self.assertIsNone(data["disk_coverage"])

    def test_pipeline_dashboard_unfindable_empty_state(self):
        """No runs yet -> the honest empty-state shape, not a 500 or a
        missing key (#1112)."""
        status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        self.assertEqual(data["unfindable"]["recent_runs"], [])
        self.assertEqual(data["unfindable"]["backlog_trend"], {
            "current_backlog": None,
            "latest_sample_at": None,
            "series": [],
        })

    def _seed_dashboard_telemetry(self) -> None:
        """Real telemetry rows for every [0]-indexed dashboard assertion:
        cycle metrics (windows + wanted-trend samples), found/loop search
        logs (match-rate series, heavy queries, loop suspects), and peer
        observations (totals + days)."""
        from datetime import timedelta
        base = datetime.now(UTC)
        self.db.record_cycle_metrics(
            cycle_total_s=300.0,
            counters=CycleCounters(
                browse_time_s=20.0, match_time_s=10.0, search_time_s=240.0,
                peers_browsed=8, fanout_waves=2,
                find_download_queued=4, find_download_completed=4,
            ),
            completed_at=base - timedelta(hours=2), wanted_total=12,
        )
        self.db.record_cycle_metrics(
            cycle_total_s=320.0,
            counters=CycleCounters(
                browse_time_s=22.0, match_time_s=11.0, search_time_s=250.0,
                peers_browsed=9, fanout_waves=3,
                find_download_queued=3, find_download_completed=3,
            ),
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
        # Real datetime-backed run row (code-quality.md § production-shape
        # mock rule) — the fake stamps ``created_at`` with a real
        # ``datetime.now(UTC)``, not a synthetic literal.
        self.db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=686,
            batch_limit=240, candidates_processed=240, probes_attempted=240,
            categorised_count=5, downgraded_count=1, no_change_count=210,
            probe_failed_count=24, breaker_tripped=False,
            duration_seconds=6961.5,
        )

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
        # Unfindable-detection run health + backlog trend (#1112).
        _assert_required_fields(
            self, data["unfindable"], self.DASHBOARD_UNFINDABLE_FIELDS,
            "pipeline dashboard unfindable")
        _assert_required_fields(
            self, data["unfindable"]["recent_runs"][0],
            self.DASHBOARD_UNFINDABLE_RUN_FIELDS,
            "pipeline dashboard unfindable recent run")
        _assert_required_fields(
            self, data["unfindable"]["backlog_trend"],
            self.DASHBOARD_UNFINDABLE_BACKLOG_TREND_FIELDS,
            "pipeline dashboard unfindable backlog trend")
        _assert_required_fields(
            self, data["unfindable"]["backlog_trend"]["series"][0],
            self.DASHBOARD_UNFINDABLE_BACKLOG_TREND_POINT_FIELDS,
            "pipeline dashboard unfindable backlog trend point")
        self.assertEqual(
            data["unfindable"]["recent_runs"][0]["due_backlog_at_start"],
            686,
        )
        self.assertEqual(
            data["unfindable"]["backlog_trend"]["current_backlog"], 686)


DASHBOARD_RETAG_DIVERGENCE_CENSUS_FIELDS = {
    "state", "error", "snapshot", "albums_shown", "albums_listed_total",
}
DASHBOARD_LIBRARY_COMPLETENESS_FIELDS = DASHBOARD_RETAG_DIVERGENCE_CENSUS_FIELDS


@contextmanager
def _retag_census_snapshot_path_set(path):
    """Derive a runtime carrying the overridden snapshot path and install
    it for the duration of the block — the #1313 runtime-value successor
    to the old plain-module-global-override convention."""
    with install_runtime(replace(runtime(), retag_census_snapshot_path=path)):
        yield


@contextmanager
def _library_completeness_snapshot_path_set(path):
    with install_runtime(
        replace(runtime(), library_completeness_snapshot_path=path),
    ):
        yield


def _snapshot(
    status: RetagDivergenceStatus = "clean",
) -> RetagDivergenceCensusSnapshot:
    return RetagDivergenceCensusSnapshot(
        generated_at="2026-08-14T09:00:00+00:00",
        duration_seconds=196.4,
        report=RetagDivergenceReport(
            status=status,
            complete=True,
            counts=RetagDivergenceCounts(0, 0, 0, 0, 0, 0, 0, 0),
            albums=(),
        ),
    )


class _PoisonedBeetsDB(FakeBeetsDB):
    """#1142 acceptance 6 — the dashboard's census card reads the
    persisted snapshot only; it must never invoke the whole-library
    reader, even when a Beets handle IS available."""

    def list_album_mb_identities(self) -> list[BeetsAlbumIdentityRow]:
        raise AssertionError(
            "dashboard must not invoke the whole-library retag-divergence "
            "reader — it reads the persisted snapshot only"
        )

    def list_library_completeness_albums(self):
        raise AssertionError(
            "dashboard must not invoke the whole-library completeness reader",
        )


class TestPipelineDashboardRetagDivergenceCensusContract(_FakeDbWebServerCase):
    """Contract tests for the ``retag_divergence_census`` dashboard block
    (#1142) — a distinct Beets-DB-vs-file-tags drift card, never the
    Disk Coverage (ledger-vs-Beets-DB) one. Read-only: the route reads
    the persisted daily snapshot, it never scans."""

    def setUp(self) -> None:
        super().setUp()
        self.db.seed_request(make_request_row(id=100, status="wanted"))

    def test_missing_state_when_no_snapshot_path_configured(self) -> None:
        with install_runtime(
            make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
        ):
            status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data["retag_divergence_census"],
            DASHBOARD_RETAG_DIVERGENCE_CENSUS_FIELDS,
            "pipeline dashboard retag divergence census",
        )
        self.assertEqual(
            data["retag_divergence_census"],
            {
                "state": "missing", "error": None, "snapshot": None,
                "albums_shown": 0, "albums_listed_total": 0,
            },
        )

    def test_missing_state_when_path_configured_but_file_absent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = retag_divergence_census_snapshot_path(tmpdir)
            with (
                _retag_census_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        self.assertEqual(data["retag_divergence_census"]["state"], "missing")

    def test_ok_state_reads_a_published_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = retag_divergence_census_snapshot_path(tmpdir)
            snapshot = _snapshot("divergence_found")
            write_retag_divergence_census_snapshot(path, snapshot)
            with (
                _retag_census_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        census = data["retag_divergence_census"]
        self.assertEqual(census["state"], "ok")
        self.assertIsNone(census["error"])
        self.assertEqual(census["snapshot"]["generated_at"], snapshot.generated_at)
        self.assertEqual(
            census["snapshot"]["duration_seconds"], snapshot.duration_seconds,
        )
        self.assertEqual(census["snapshot"]["report"]["status"], "divergence_found")
        # An uncapped small report — shown equals the true total.
        self.assertEqual(census["albums_shown"], len(snapshot.report.albums))
        self.assertEqual(
            census["albums_listed_total"], len(snapshot.report.albums),
        )

    def test_ok_state_caps_the_embedded_album_list(self) -> None:
        """N1 (fresh review) — the PERSISTED snapshot may legitimately
        list every non-agreeing album (a read-failure-heavy world could
        mean thousands), but the DASHBOARD route's own JSON projection
        must never serialize more than
        ``DASHBOARD_RETAG_CENSUS_ALBUM_CAP`` of them — no silent
        truncation: albums_shown/albums_listed_total tell the caller
        exactly what happened. The persisted file on disk is untouched
        (this route only reads it)."""
        from web.routes.pipeline_dashboard import (
            DASHBOARD_RETAG_CENSUS_ALBUM_CAP,
        )

        total_albums = DASHBOARD_RETAG_CENSUS_ALBUM_CAP + 7
        albums = tuple(
            RetagDivergenceAlbum(
                album_id=i, db_mb_albumid=f"mb-{i}",
                album_class="diverges", item_count=0, items=(),
            )
            for i in range(1, total_albums + 1)
        )
        snapshot = RetagDivergenceCensusSnapshot(
            generated_at="2026-08-14T09:00:00+00:00",
            duration_seconds=1.0,
            report=RetagDivergenceReport(
                status="divergence_found", complete=True,
                counts=RetagDivergenceCounts(
                    total_albums, 0, 0, 0, total_albums, 0, 0, 0,
                ),
                albums=albums,
            ),
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            path = retag_divergence_census_snapshot_path(tmpdir)
            write_retag_divergence_census_snapshot(path, snapshot)
            with (
                _retag_census_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")

            # The persisted file on disk still has every album — this
            # route only ever reads it, never rewrites the cap back to
            # disk. Read back while tmpdir is still alive.
            read_back = read_retag_divergence_census_snapshot(path)

        self.assertEqual(status, 200)
        census = data["retag_divergence_census"]
        self.assertEqual(census["state"], "ok")
        # Never serializes beyond the cap.
        self.assertEqual(
            len(census["snapshot"]["report"]["albums"]),
            DASHBOARD_RETAG_CENSUS_ALBUM_CAP,
        )
        self.assertEqual(census["albums_shown"], DASHBOARD_RETAG_CENSUS_ALBUM_CAP)
        self.assertEqual(census["albums_listed_total"], total_albums)
        # The existing honest whole-library counts are untouched by the
        # projection cap — still the true, uncapped numbers.
        self.assertEqual(
            census["snapshot"]["report"]["counts"]["albums_scanned"],
            total_albums,
        )
        self.assertEqual(census["snapshot"]["report"]["status"], "divergence_found")
        assert read_back is not None
        self.assertEqual(len(read_back.report.albums), total_albums)

    def test_unreadable_state_on_malformed_snapshot_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = retag_divergence_census_snapshot_path(tmpdir)
            with open(path, "wb") as fh:
                fh.write(b"not json at all")
            with (
                _retag_census_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
                self.assertLogs(
                    "web.routes.pipeline_dashboard", level="ERROR",
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")

        # A corrupt census card must never 500 the whole dashboard.
        self.assertEqual(status, 200)
        census = data["retag_divergence_census"]
        self.assertEqual(census["state"], "unreadable")
        self.assertIsNone(census["snapshot"])
        self.assertIsNotNone(census["error"])


class TestPipelineDashboardLibraryCompletenessContract(_FakeDbWebServerCase):
    def setUp(self) -> None:
        super().setUp()
        self.db.seed_request(make_request_row(id=100, status="wanted"))

    def test_snapshot_is_capped_without_changing_persisted_totals(self) -> None:
        from web.routes.pipeline_dashboard import (
            DASHBOARD_LIBRARY_COMPLETENESS_ALBUM_CAP,
        )
        count = DASHBOARD_LIBRARY_COMPLETENESS_ALBUM_CAP + 3
        snapshot = LibraryCompletenessSnapshot(
            "2026-08-17T00:00:00+00:00", 2.0,
            CompletenessReport("incomplete", CompletenessCounts(count, 0, count, 0, 0), tuple(
                CompletenessAlbum(i, "Artist", f"Album {i}", "release", (CompletenessFinding("missing_source_audio", "track"),), 1, 0, 0)
                for i in range(count)
            )),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            write_library_completeness_snapshot(path, snapshot)
            with (
                _library_completeness_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")
            persisted = read_library_completeness_snapshot(path)
        self.assertEqual(status, 200)
        block = data["library_completeness"]
        _assert_required_fields(self, block, DASHBOARD_LIBRARY_COMPLETENESS_FIELDS, "library completeness")
        self.assertEqual(block["state"], "ok")
        self.assertEqual(block["albums_shown"], DASHBOARD_LIBRARY_COMPLETENESS_ALBUM_CAP)
        self.assertEqual(block["albums_listed_total"], count)
        self.assertEqual(block["snapshot"]["report"]["counts"]["missing_source_audio"], count)
        self.assertNotIn(
            "non_audio_omitted", block["snapshot"]["report"]["counts"],
        )
        assert persisted is not None
        self.assertEqual(len(persisted.report.albums), count)

    def test_missing_snapshot_never_scans_beets(self) -> None:
        with install_runtime(
            make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
        ):
            status, data = self._get("/api/pipeline/dashboard")
        self.assertEqual(status, 200)
        self.assertEqual(data["library_completeness"], {
            "state": "missing", "error": None, "snapshot": None,
            "albums_shown": 0, "albums_listed_total": 0,
        })

    def test_albums_are_enriched_with_request_and_mark_state(self) -> None:
        """Issue #1241: each embedded census album carries the resolved
        pipeline ``request_id`` and its ``marked_incomplete`` state so the
        card can offer the mark/clear action inline; a census album with no
        resolvable request carries ``request_id=None``."""
        self.db.seed_request(make_request_row(
            id=310, status="imported", mb_release_id="rel-marked",
        ))
        self.db.seed_request(make_request_row(
            id=311, status="imported", mb_release_id="rel-unmarked",
        ))
        self.db.set_marked_incomplete(310, marked=True)
        snapshot = LibraryCompletenessSnapshot(
            "2026-08-25T00:00:00+00:00", 2.0,
            CompletenessReport(
                "incomplete", CompletenessCounts(3, 0, 3, 0, 0), (
                    CompletenessAlbum(
                        1, "Artist", "Marked", "rel-marked",
                        (CompletenessFinding("missing_source_audio", "t"),),
                        1, 0, 0),
                    CompletenessAlbum(
                        2, "Artist", "Unmarked", "rel-unmarked",
                        (CompletenessFinding("missing_source_audio", "t"),),
                        1, 0, 0),
                    CompletenessAlbum(
                        3, "Artist", "No request", "rel-unknown",
                        (CompletenessFinding("missing_source_audio", "t"),),
                        1, 0, 0),
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            write_library_completeness_snapshot(path, snapshot)
            with (
                _library_completeness_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")
        self.assertEqual(status, 200)
        albums = data["library_completeness"]["snapshot"]["report"]["albums"]
        by_release = {album["release_id"]: album for album in albums}
        self.assertEqual(by_release["rel-marked"]["request_id"], 310)
        self.assertTrue(by_release["rel-marked"]["marked_incomplete"])
        self.assertEqual(by_release["rel-unmarked"]["request_id"], 311)
        self.assertFalse(by_release["rel-unmarked"]["marked_incomplete"])
        self.assertIsNone(by_release["rel-unknown"]["request_id"])
        self.assertFalse(by_release["rel-unknown"]["marked_incomplete"])

    def test_one_refused_presentation_row_degrades_one_album_only(self) -> None:
        """#1257 review F9: the presentation projection refuses a
        ``processing`` row missing its owner join (RuntimeError). One such
        row must degrade that one album to actionless — never 500 the
        whole dashboard route."""
        self.db.seed_request(make_request_row(
            id=320, status="imported", mb_release_id="rel-healthy",
        ))
        # Real refused shape: processing with no attached automation owner.
        self.db.seed_request(make_request_row(
            id=321, status="processing", mb_release_id="rel-broken",
        ))
        snapshot = LibraryCompletenessSnapshot(
            "2026-08-25T00:00:00+00:00", 2.0,
            CompletenessReport(
                "incomplete", CompletenessCounts(2, 0, 2, 0, 0), (
                    CompletenessAlbum(
                        1, "Artist", "Healthy", "rel-healthy",
                        (CompletenessFinding("missing_source_audio", "t"),),
                        1, 0, 0),
                    CompletenessAlbum(
                        2, "Artist", "Broken", "rel-broken",
                        (CompletenessFinding("missing_source_audio", "t"),),
                        1, 0, 0),
                ),
            ),
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            write_library_completeness_snapshot(path, snapshot)
            # An unavailable shared Beets handle skips the Disk Coverage
            # card, whose own unguarded presentation read would otherwise
            # trip over the broken row first — this test isolates the
            # completeness enrichment's per-album guard.
            with (
                _library_completeness_snapshot_path_set(path),
                install_runtime(replace(runtime(), shared_beets=None)),
                self.assertLogs(
                    "web.routes.pipeline_dashboard", level="ERROR"),
            ):
                status, data = self._get("/api/pipeline/dashboard")
        self.assertEqual(status, 200)
        albums = data["library_completeness"]["snapshot"]["report"]["albums"]
        by_release = {album["release_id"]: album for album in albums}
        self.assertEqual(by_release["rel-healthy"]["request_id"], 320)
        self.assertIsNone(by_release["rel-broken"]["request_id"])
        self.assertFalse(by_release["rel-broken"]["marked_incomplete"])

    def test_unreadable_snapshot_is_in_band_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            with open(path, "wb") as fh:
                fh.write(b"broken")
            with (
                _library_completeness_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
                self.assertLogs("web.routes.pipeline_dashboard", level="ERROR"),
            ):
                status, data = self._get("/api/pipeline/dashboard")
        self.assertEqual(status, 200)
        self.assertEqual(data["library_completeness"]["state"], "unreadable")

    def test_unreadable_state_on_permission_error(self) -> None:
        """N4 (#1142 review) — a filesystem-level read failure (denied
        permissions, or the path resolving to a directory) must ALSO
        never 500 the whole dashboard, not just a content-decode
        failure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = retag_divergence_census_snapshot_path(tmpdir)
            write_retag_divergence_census_snapshot(path, _snapshot("clean"))
            with (
                _retag_census_snapshot_path_set(path),
                install_runtime(
                    make_web_runtime(runtime(), beets=_PoisonedBeetsDB()),
                ),
                patch(
                    "lib.retag_divergence_census_snapshot.open",
                    side_effect=PermissionError("denied"),
                ),
                self.assertLogs(
                    "web.routes.pipeline_dashboard", level="ERROR",
                ),
            ):
                status, data = self._get("/api/pipeline/dashboard")

        self.assertEqual(status, 200)
        census = data["retag_divergence_census"]
        self.assertEqual(census["state"], "unreadable")
        self.assertIsNone(census["snapshot"])
        self.assertIsNotNone(census["error"])




LIBRARY_CENSUS_REFRESH_FIELDS = {"outcome", "error"}


class TestLibraryCensusRefreshContract(_FakeDbWebServerCase):
    """POST /api/pipeline/dashboard/library-census/refresh — the census
    force button. Writes the trigger file the module's path unit
    watches; the daily oneshot stays the single execution path."""

    def test_refresh_writes_trigger_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            with _library_completeness_snapshot_path_set(path):
                status, data = self._post(
                    "/api/pipeline/dashboard/library-census/refresh", {},
                )
            trigger = library_completeness_trigger_path(tmpdir)
            trigger_exists = os.path.exists(trigger)
        self.assertEqual(status, 200)
        _assert_required_fields(
            self, data, LIBRARY_CENSUS_REFRESH_FIELDS, "census refresh",
        )
        self.assertEqual(data["outcome"], "requested")
        self.assertIsNone(data["error"])
        self.assertTrue(trigger_exists)

    def test_refresh_is_idempotent_while_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = library_completeness_snapshot_path(tmpdir)
            with _library_completeness_snapshot_path_set(path):
                first = self._post(
                    "/api/pipeline/dashboard/library-census/refresh", {},
                )
                second = self._post(
                    "/api/pipeline/dashboard/library-census/refresh", {},
                )
        self.assertEqual(first[0], 200)
        self.assertEqual(second[0], 200)
        self.assertEqual(second[1]["outcome"], "requested")

    def test_unconfigured_snapshot_path_returns_503(self) -> None:
        with _library_completeness_snapshot_path_set(None):
            status, data = self._post(
                "/api/pipeline/dashboard/library-census/refresh", {},
            )
        self.assertEqual(status, 503)
        self.assertEqual(data["outcome"], "unconfigured")

    def test_unwritable_state_dir_returns_503(self) -> None:
        # The real adapter raises OSError on an unwritable dir (Rule B:
        # the failure-case shape is the real exception, not a stand-in).
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chmod(tmpdir, 0o500)
            try:
                path = library_completeness_snapshot_path(tmpdir)
                with _library_completeness_snapshot_path_set(path):
                    status, data = self._post(
                        "/api/pipeline/dashboard/library-census/refresh", {},
                    )
            finally:
                os.chmod(tmpdir, 0o700)
        self.assertEqual(status, 503)
        self.assertEqual(data["outcome"], "unavailable")
        self.assertTrue(data["error"])


if __name__ == "__main__":
    unittest.main()
