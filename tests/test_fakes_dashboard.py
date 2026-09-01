"""Self-tests for the FakePipelineDB dashboard-telemetry cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

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


if __name__ == "__main__":
    unittest.main()
