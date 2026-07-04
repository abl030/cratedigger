#!/usr/bin/env python3
"""Contract tests for the search-plan routes (web/routes/search_plan.py).

Split from tests/test_web_server.py (#408). Moved from
tests/web/test_routes_pipeline_search_plan.py to mirror
web/routes/search_plan.py's own split out of web/routes/pipeline.py
(#481 item 3). Shared harness in tests/web/_harness.py.
"""

from datetime import datetime, timedelta, timezone
import json
import os
import sys
import unittest
from unittest.mock import patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _FakeDbWebServerCase

from tests.helpers import make_request_row


class TestPipelineSearchPlanContract(_FakeDbWebServerCase):
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

    def _seed_request(self, *, request_id: int = 100,
                      status: str = "wanted") -> None:
        self.db.seed_request(make_request_row(id=request_id, status=status))

    def _plan_items(self, items_count: int = 2):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(
                ordinal=i,
                strategy=("default" if i == 0 else f"strategy_{i}"),
                query=f"q{i}", canonical_query_key=f"k{i}",
                repeat_group=("default-3" if i == 0 else None),
                provenance={"src": "gen"} if i == 0 else None,
            )
            for i in range(items_count)
        ]

    def _seed_active_plan(
        self, *,
        generator_id: str = "search-plan/2026-05-25-1",
        items_count: int = 2,
        plan_provenance: dict | None = None,
    ) -> int:
        return self.db.create_successful_search_plan(
            request_id=100, generator_id=generator_id,
            items=self._plan_items(items_count),
            metadata_snapshot={"artist_name": "X"},
            provenance=plan_provenance,
        )

    def _seed_failed_plan(
        self, *,
        transient: bool,
        failure_class: str,
        error_message: str = "boom",
        generator_id: str = "search-plan/2026-05-19-1",
    ) -> int:
        return self.db.create_failed_search_plan(
            request_id=100, generator_id=generator_id,
            failure_class=failure_class, error_message=error_message,
            transient=transient,
            provenance={"reason": failure_class},
        )

    def _seed_legacy_logs(self, n: int) -> None:
        """Plan-less search_log rows — the fake counts plan_id IS NULL
        rows as legacy, same as production."""
        for i in range(1, n + 1):
            self.db.log_search(
                100, query=f"q{i}",
                outcome=("found" if i % 2 == 0 else "no_match"),
                variant="v1", result_count=(5 if i % 2 == 0 else 0),
                elapsed_s=float(i), final_state="Completed",
            )

    # -- happy path: active + failures + legacy logs --

    def test_search_plan_route_returns_full_inspection_payload(self):
        self._seed_request()
        # Three real generations: two get superseded, the third is the
        # current active plan — superseded_count comes from real rows.
        self._seed_active_plan()
        self.db.supersede_search_plan_with_replacement(
            request_id=100, generator_id="search-plan/2026-05-25-1",
            items=self._plan_items(),
            metadata_snapshot={"artist_name": "X"},
        )
        self.db.supersede_search_plan_with_replacement(
            request_id=100, generator_id="search-plan/2026-05-25-1",
            items=self._plan_items(),
            metadata_snapshot={"artist_name": "X"},
            provenance={
                "omitted_candidates": [{"q": "x", "why": "low_entropy"}],
                "dropped_low_entropy_tokens": ["the", "and"],
            },
        )
        self._seed_failed_plan(
            transient=False, failure_class="no_runnable_query",
            error_message="all queries dropped")
        self._seed_failed_plan(
            transient=True, failure_class="resolver_unavailable",
            error_message="resolver 503")
        # 7 plan-less rows: the head must cap at the route's limit of 5
        # while the count reflects all 7.
        self._seed_legacy_logs(7)

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
        self.assertEqual(data["legacy_logs"]["count"], 7)
        # The head is capped at the route's limit (5) even though all
        # 7 rows are counted — pins the limit the route passes down.
        self.assertEqual(len(data["legacy_logs"]["head"]), 5)
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
        status, data = self._get("/api/pipeline/9999/search-plan")
        self.assertEqual(status, 404)
        self.assertIn("error", data)

    # -- request with no plan at all (transient failure) --

    def test_search_plan_no_active_plan_with_only_transient_failure(self):
        self._seed_request()
        self._seed_failed_plan(
            transient=True, failure_class="resolver_unavailable")
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertIsNone(data["active_plan"])
        self.assertFalse(data["currentness"]["has_active_plan"])
        self.assertFalse(data["currentness"]["current_generator_searchable"])
        self.assertTrue(data["currentness"]["has_retryable_failure"])

    # -- legacy logs section is non-empty when only legacy rows exist --

    def test_search_plan_surfaces_legacy_logs_when_no_plan_context(self):
        self._seed_request()
        self._seed_legacy_logs(2)
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertEqual(data["legacy_logs"]["count"], 2)
        self.assertEqual(len(data["legacy_logs"]["head"]), 2)

    # -- generator id drift: active plan but stale generator id --

    def test_search_plan_flags_generator_id_mismatch_when_stale(self):
        self._seed_request()
        self._seed_active_plan(generator_id="search-plan/2026-01-01-old")
        status, data = self._get("/api/pipeline/100/search-plan")
        self.assertEqual(status, 200)
        self.assertTrue(data["currentness"]["has_active_plan"])
        self.assertTrue(data["currentness"]["generator_id_mismatch"])
        self.assertFalse(data["currentness"]["current_generator_searchable"])
        self.assertEqual(
            data["currentness"]["active_plan_generator_id"],
            "search-plan/2026-01-01-old")


class TestPipelineSearchPlanDryRunContract(_FakeDbWebServerCase):
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
        super().setUp()
        self.db.seed_request(make_request_row(
            id=100, status="wanted",
            artist_name="Radiohead", album_title="Kid A",
            year=2008, release_group_year=2000,
        ))
        self.db.set_tracks(100, [
            {"disc_number": 1, "track_number": 1,
             "title": "Everything In Its Right Place"},
            {"disc_number": 1, "track_number": 2, "title": "Kid A"},
            {"disc_number": 1, "track_number": 3,
             "title": "The National Anthem"},
        ])
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
        from lib.pipeline_db import SearchPlanItemInput
        self.db.create_successful_search_plan(
            request_id=100, generator_id="search-plan/2026-05-25-1",
            items=[SearchPlanItemInput(
                ordinal=0, strategy="default", query="radiohead kid a")],
        )
        status, data = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.assertTrue(data["would_supersede_active"])

    def test_dry_run_request_not_found_returns_404(self):
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
        self.db.set_tracks(100, [])
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
        # The simulator must never persist anything: after the call the
        # fake's plan store is still empty and the request has no
        # active plan — the domain version of "no write methods ran".
        status, _ = self._get(
            "/api/pipeline/100/search-plan/dry-run")
        self.assertEqual(status, 200)
        self.assertEqual(self.db.search_plans, {})
        self.assertIsNone(self.db.get_active_search_plan(100))

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


class TestPipelineSearchPlanSaturationContract(_FakeDbWebServerCase):
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
        super().setUp()
        import uuid
        self.db.seed_request(make_request_row(
            id=100, status="wanted",
            artist_name="Radiohead", album_title="Kid A",
            mb_release_id=str(uuid.uuid4()),
        ))
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

    def tearDown(self) -> None:
        self._cfg_patcher.stop()

    def _seed_search_logs(self, *, total: int, saturated: int,
                          skips_total: int) -> None:
        """Real search_log rows the fake's get_saturation_summary
        aggregates: ``LimitReached`` in final_state counts as
        saturated; pre_filter_skip_count sums."""
        per, rem = divmod(skips_total, total)
        for i in range(total):
            self.db.log_search(
                100, query=f"q{i}", outcome="no_match",
                final_state=(
                    "Completed, LimitReached" if i < saturated
                    else "Completed"),
                pre_filter_skip_count=per + (1 if i < rem else 0),
            )

    def _age_last_log(self, days: int) -> None:
        self.db.search_logs[-1].created_at -= timedelta(days=days)

    def test_happy_path_returns_200_with_required_fields(self):
        self._seed_search_logs(total=30, saturated=10, skips_total=50)
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
        # Found-but-quiet: request exists, no search_log rows at all.
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation")
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "success")
        # Crucial: rate is 0.0, NOT NaN — JSON parses cleanly.
        self.assertEqual(data["saturation_rate"], 0.0)
        self.assertEqual(data["total_searches"], 0)

    def test_request_not_found_returns_404(self):
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

    def test_window_days_query_string_propagates_to_service(self):
        self._seed_search_logs(total=5, saturated=1, skips_total=3)
        # One older row inside the default 14d window but OUTSIDE the
        # requested 7d window — if the route failed to propagate
        # window_days to the aggregator, this row would inflate the
        # totals below.
        self.db.log_search(
            100, query="old", outcome="no_match", final_state="Completed")
        self._age_last_log(10)
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation?window_days=7")
        self.assertEqual(status, 200)
        self.assertEqual(data["window_days"], 7)
        self.assertEqual(data["total_searches"], 5)
        self.assertEqual(data["saturated_searches"], 1)
        self.assertAlmostEqual(data["saturation_rate"], 0.2)
        self.assertEqual(data["total_pre_filter_skips"], 3)

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
        # The 10-day-old row IS inside the default 14d window — the
        # complement of the 7d propagation test above.
        self.db.log_search(
            100, query="old", outcome="no_match", final_state="Completed")
        self._age_last_log(10)
        status, data = self._get(
            "/api/pipeline/100/search-plan/saturation")
        self.assertEqual(status, 200)
        self.assertEqual(data["window_days"], 14)
        self.assertEqual(data["total_searches"], 1)


class TestPipelineSearchPlanRegenerateContract(_FakeDbWebServerCase):
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
        super().setUp()
        self.db.seed_request(make_request_row(
            id=100, status="wanted",
        ))
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
        self.db.seed_request(make_request_row(
            id=100, status="imported",
        ))
        with self._patch_service(outcome="success", plan_id=99):
            status, data = self._post(
                "/api/pipeline/100/search-plan/regenerate", {})
        self.assertEqual(status, 200)
        self.assertEqual(data["outcome"], "success")
        self.assertFalse(data["executable"])
        self.assertEqual(data["request_status"], "imported")

    def test_regenerate_request_not_found_returns_404(self):
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
                with urlopen(req, timeout=5) as resp:
                    status = resp.status
                    data = json.loads(resp.read())
            except HTTPError as e:
                with e:
                    status = e.code
                    data = json.loads(e.read())
        self.assertEqual(status, 400)
        self.assertIn("error", data)
        mock_gen.assert_not_called()


class TestPipelineSearchPlanAdvanceContract(_FakeDbWebServerCase):
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
        super().setUp()
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


class TestPipelineSearchPlanHistoryContract(_FakeDbWebServerCase):
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
        super().setUp()
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

if __name__ == "__main__":
    unittest.main()
