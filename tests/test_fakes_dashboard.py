"""Self-tests for the FakePipelineDB dashboard-telemetry cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

from tests.dispatch_helpers import handoff_automation_owner
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakeDashboardMirror(unittest.TestCase):
    """The dashboard read-model mirror aggregates real seeded telemetry
    and must emit a fully JSON-serializable envelope (production
    isoformats every timestamp at the _isoformat_or_none boundary —
    a raw datetime here 500s the dashboard route)."""

    def _seeded_db(self) -> FakePipelineDB:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.record_cycle_metrics(
            cycle_total_s=300.0, search_time_s=240.0, peers_browsed=8,
            find_download_queued=4, find_download_completed=4,
            wanted_total=10,
        )
        db.log_search(
            1, query="q", outcome="found", result_count=5, elapsed_s=2.0,
            variant="v1", final_state="Completed", browse_time_s=42.0,
            peers_browsed=110, peers_browsed_lazy=5, fanout_waves=6,
        )
        db.log_search(1, query="q2", outcome="no_match", elapsed_s=1.0)
        db.record_peer_observations(["peer-a", "peer-b"])
        return db

    def test_envelope_is_json_serializable_with_seeded_telemetry(self):
        import json
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        json.dumps(payload)  # raises TypeError on any leaked datetime

    def test_windows_and_coverage_aggregate_seeded_rows(self):
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        win24 = payload["searches"]["windows"][0]
        self.assertEqual(win24["label"], "24h")
        self.assertEqual(win24["searches"], 2)
        self.assertEqual(win24["outcomes"]["found"], 1)
        self.assertEqual(win24["outcomes"]["no_match"], 1)
        cov = payload["coverage"]
        self.assertEqual(cov["matches_24h"], 1)
        self.assertEqual(cov["wanted_total"], 1)
        self.assertEqual(cov["wanted_searched_24h"], 1)
        # Production zero-fills the series via generate_series — DENSE:
        # always exactly 24 hourly / 28 daily buckets.
        self.assertEqual(len(cov["match_rate_series_24h"]), 24)
        self.assertEqual(len(cov["match_rate_series_28d"]), 28)
        self.assertEqual(
            sum(pt["matches"] for pt in cov["match_rate_series_24h"]), 1)
        # Heavy-query panel surfaces the browse-heavy row.
        heavy = payload["peers"]["heavy_queries"]
        self.assertEqual(len(heavy), 1)
        self.assertEqual(heavy[0]["peers_browsed"], 110)
        self.assertEqual(heavy[0]["peer_dirs"], 115)
        cyc24 = payload["cycles"]["windows"][0]
        self.assertEqual(cyc24["cycles"], 1)
        self.assertEqual(cyc24["find_download_queued"], 4)

    def test_empty_db_emits_complete_envelope(self):
        import json
        payload = FakePipelineDB().get_pipeline_dashboard_metrics()
        json.dumps(payload)
        self.assertEqual(payload["searches"]["windows"][0]["searches"], 0)
        self.assertEqual(payload["coverage"]["wanted_total"], 0)
        self.assertEqual(payload["peers"]["heavy_queries"], [])
        # Dense zero-filled series even with zero telemetry.
        self.assertEqual(
            len(payload["coverage"]["match_rate_series_24h"]), 24)
        self.assertEqual(
            len(payload["coverage"]["match_rate_series_28d"]), 28)
        # Never null — production emits 0 when there are no searches.
        self.assertEqual(payload["coverage"]["top_10_share_24h"], 0)

    def test_cycle_rows_use_production_serializer_keys(self):
        """recent/outliers rows carry the renamed watchdog_kills key and
        NOT the raw cycle_metrics column names production never emits."""
        db = self._seeded_db()
        payload = db.get_pipeline_dashboard_metrics()
        recent = payload["cycles"]["recent"]
        self.assertEqual(len(recent), 1)
        row = recent[0]
        self.assertIn("watchdog_kills", row)
        self.assertNotIn("cycle_searches_watchdog_killed", row)
        self.assertNotIn("cache_pos_hits", row)
        self.assertNotIn("wanted_total", row)
        self.assertIsInstance(row["created_at"], str)

    def test_exhausted_outcome_counts_as_reset_in_suspects(self):
        """Production's reset_24h counts the HISTORICAL ``exhausted``
        outcome; problem_24h is restricted to timeout/error/empty_query."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_search(1, query="q", outcome="exhausted")
        db.log_search(1, query="q", outcome="timeout")
        db.log_search(1, query="q", outcome="some_unknown_outcome")
        payload = db.get_pipeline_dashboard_metrics()
        suspects = payload["coverage"]["top_loop_suspects"]
        self.assertEqual(len(suspects), 1)
        self.assertEqual(suspects[0]["reset_24h"], 1)
        self.assertEqual(suspects[0]["problem_24h"], 1)
        # Search-window errors bucket mirrors the SQL FILTER: the
        # unknown outcome counts toward searches but NO bucket.
        win24 = payload["searches"]["windows"][0]
        self.assertEqual(win24["searches"], 3)
        self.assertEqual(win24["outcomes"]["errors"], 1)
        self.assertEqual(win24["outcomes"]["exhausted"], 1)

    def test_stale_wanted_includes_recently_searched_and_caps_at_12(self):
        """Production's stale panel is the 12 oldest-searched backlog
        rows ordered last_search_at ASC NULLS FIRST — recently-searched
        rows are included, never-searched rows sort first."""
        db = FakePipelineDB()
        for rid in range(1, 15):
            db.seed_request(make_request_row(id=rid, status="wanted"))
        db.log_search(1, query="q", outcome="no_match")  # searched 1h ago
        payload = db.get_pipeline_dashboard_metrics()
        stale = payload["coverage"]["stale_wanted"]
        self.assertEqual(len(stale), 12)
        # Never-searched rows lead; the searched row sorts last and IS
        # eligible (it would be excluded only by the LIMIT, with 14
        # backlog rows it falls off the end).
        self.assertIsNone(stale[0]["last_search_at"])
        self.assertNotIn(1, [r["request_id"] for r in stale])

    def test_heavy_queries_lazy_only_rows_qualify(self):
        """The filter is (peers_browsed + peers_browsed_lazy) > 0 — a
        lazy-only browse row qualifies; result_count coerces to int."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.log_search(1, query="lazy", outcome="no_match",
                      peers_browsed_lazy=7)
        payload = db.get_pipeline_dashboard_metrics()
        heavy = payload["peers"]["heavy_queries"]
        self.assertEqual(len(heavy), 1)
        self.assertEqual(heavy[0]["peer_dirs"], 7)
        self.assertEqual(heavy[0]["result_count"], 0)


class TestFakeDashboardMetricStubs(unittest.TestCase):
    """The dashboard metric stubs and the unfindable-run metrics row that
    feeds them.
    """

    def test_dashboard_metric_stubs_return_core_shapes(self):
        db = FakePipelineDB()

        cycle_id = db.record_cycle_metrics(cycle_total_s=12.5)
        new_peers = db.record_peer_observations(["alice", "alice", "bob"])
        repeated = db.record_peer_observations(["alice"])

        self.assertEqual(cycle_id, 1)
        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 0)
        self.assertEqual(new_peers, 2)
        self.assertEqual(repeated, 0)
        peer_metrics = db.get_peer_metrics()
        self.assertEqual(peer_metrics["totals"]["known_peers"], 2)
        dashboard = db.get_pipeline_dashboard_metrics()
        self.assertIn("cycles", dashboard)
        self.assertEqual(dashboard["cycles"]["recent"][0]["cycle_total_s"],
                         12.5)
        self.assertEqual(dashboard["peers"]["totals"]["known_peers"], 2)
        self.assertEqual(
            dashboard["coverage"]["wanted_trend"]["current_wanted"], 0)

    def test_dashboard_wanted_total_includes_downloading_and_processing(self):
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1, status="wanted"))
        db.seed_request(make_request_row(id=2, status="downloading"))
        db.seed_request(make_request_row(id=3, status="imported"))
        processing_id = db.add_request(
            "Artist",
            "Processing",
            "request",
            mb_release_id="fake-dashboard-processing",
        )
        handoff_automation_owner(db, processing_id)

        db.record_cycle_metrics(cycle_total_s=1.0)
        dashboard = db.get_pipeline_dashboard_metrics()

        self.assertEqual(db.cycle_metrics[0]["wanted_total"], 3)
        self.assertEqual(
            dashboard["coverage"]["wanted_trend"]["current_wanted"], 3)

    def test_unfindable_run_metrics_stub_round_trips_and_feeds_dashboard(self):
        db = FakePipelineDB()

        empty = db.get_pipeline_dashboard_metrics()["unfindable"]
        self.assertEqual(empty["recent_runs"], [])
        self.assertIsNone(empty["backlog_trend"]["current_backlog"])

        first_id = db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=900,
            batch_limit=240, candidates_processed=240, probes_attempted=240,
            categorised_count=5, downgraded_count=1, no_change_count=210,
            probe_failed_count=24, breaker_tripped=False,
            duration_seconds=6900.0,
        )
        second_id = db.record_unfindable_run_metrics(
            cohort_total=1301, due_backlog_at_start=686,
            batch_limit=240, candidates_processed=93, probes_attempted=90,
            probe_failed_count=90, not_due_count=0,
            request_not_found_count=3, breaker_tripped=True,
            duration_seconds=1800.0,
        )
        self.assertEqual((first_id, second_id), (1, 2))

        rows = db.get_unfindable_run_metrics(limit=5)
        self.assertEqual(len(rows), 2)
        # Newest first, and every field of the second call round-trips.
        newest = rows[0]
        self.assertEqual(newest["id"], second_id)
        self.assertEqual(newest["due_backlog_at_start"], 686)
        self.assertEqual(newest["candidates_processed"], 93)
        self.assertEqual(newest["probes_attempted"], 90)
        self.assertEqual(newest["probe_failed_count"], 90)
        self.assertEqual(newest["request_not_found_count"], 3)
        self.assertTrue(newest["breaker_tripped"])
        self.assertEqual(newest["duration_seconds"], 1800.0)
        self.assertEqual(newest["categorised_count"], 0)

        dashboard = db.get_pipeline_dashboard_metrics()["unfindable"]
        self.assertEqual(len(dashboard["recent_runs"]), 2)
        self.assertEqual(
            dashboard["recent_runs"][0]["due_backlog_at_start"], 686)
        self.assertEqual(dashboard["backlog_trend"]["current_backlog"], 686)
        self.assertEqual(
            [pt["due_backlog_at_start"]
             for pt in dashboard["backlog_trend"]["series"]],
            [900, 686],
        )

    def test_record_unfindable_run_metrics_rejects_non_partitioning_counts(
        self,
    ):
        """Mirror of unfindable_run_metrics_partition_check (migration
        077, #1112 review round 2 R5) -- the six RESULT_* outcome counts
        must sum to candidates_processed exactly."""
        import psycopg2.errors
        db = FakePipelineDB()
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5, probes_attempted=5,
                breaker_tripped=False, duration_seconds=1.0,
                categorised_count=1, no_change_count=1,  # sums to 2, not 5
            )

    def test_record_unfindable_run_metrics_rejects_wrong_probes_attempted(
        self,
    ):
        """Mirror of unfindable_run_metrics_probes_attempted_check
        (migration 077, #1112 review round 2 R5) -- probes_attempted
        must equal candidates_processed minus not_due_count minus
        request_not_found_count."""
        import psycopg2.errors
        db = FakePipelineDB()
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.record_unfindable_run_metrics(
                cohort_total=10, due_backlog_at_start=5,
                batch_limit=5, candidates_processed=5,
                probes_attempted=5,  # should be 5 - 0 - 2 = 3
                breaker_tripped=False, duration_seconds=1.0,
                no_change_count=3, request_not_found_count=2,
            )


class TestFakePeerMetrics(unittest.TestCase):
    """The peer_observations roster mirror (#227): cumulative totals carry
    forward, and buckets follow the local date rather than UTC.
    """

    def test_peer_metrics_cumulative_totals_carry_forward(self):
        """``total_peers`` accumulates across days and carries forward
        over days with no new peers."""
        db = FakePipelineDB()
        now = datetime.now(UTC)
        db.record_peer_observations(
            ["old1", "old2"], observed_at=now - timedelta(days=5))
        db.record_peer_observations(["new1"], observed_at=now)

        resp = db.get_peer_metrics(days=14)
        self.assertEqual(resp["totals"]["known_peers"], 3)
        self.assertEqual(resp["totals"]["new_24h"], 1)
        self.assertEqual(resp["totals"]["seen_24h"], 1)
        self.assertEqual(resp["days"][0]["total_peers"], 3)
        self.assertEqual(resp["days"][1]["total_peers"], 2)
        self.assertEqual(
            sum(d["new_peers"] for d in resp["days"]), 3)

    def test_peer_metrics_buckets_by_perth_local_date_not_utc(self):
        """Perth-boundary regression: ``2026-05-07 23:55 UTC`` is
        ``2026-05-08 07:55 Perth``. The fake must bucket it into
        2026-05-08, matching the real method's
        ``(first_seen_at AT TIME ZONE 'Australia/Perth')::date``
        expression."""
        db = FakePipelineDB()
        perth = ZoneInfo("Australia/Perth")
        observed_at = datetime(
            2026, 5, 7, 23, 55, tzinfo=UTC,
        )
        # Sanity: the same instant in Perth-local is 2026-05-08 07:55.
        self.assertEqual(observed_at.astimezone(perth).date(),
                         date(2026, 5, 8))

        db.record_peer_observations(["alice"], observed_at=observed_at)

        with patch("tests.fakes.pipeline_db.dashboard._utcnow") as fake_now:
            fake_now.return_value = datetime(
                2026, 5, 9, 5, 0, tzinfo=UTC,
            )  # 2026-05-09 13:00 Perth
            resp = db.get_peer_metrics(days=14)

        by_date = {r["date"]: r for r in resp["days"]}
        self.assertEqual(by_date["2026-05-08"]["new_peers"], 1)
        self.assertEqual(by_date["2026-05-07"]["new_peers"], 0)
        self.assertEqual(by_date["2026-05-07"]["total_peers"], 0)
        self.assertEqual(by_date["2026-05-08"]["total_peers"], 1)


if __name__ == "__main__":
    unittest.main()
