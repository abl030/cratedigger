#!/usr/bin/env python3
"""Contract tests for the search-plan routes (web/routes/pipeline.py).

Split from tests/test_web_server.py (#408). Shared harness in
tests/web/_harness.py.
"""

from datetime import datetime, timezone
import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch
from urllib.request import urlopen, Request
from urllib.error import HTTPError


sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from tests.web._harness import _assert_required_fields, _WebServerCase

from lib.pipeline_db import SearchPlanProvenance
from tests.helpers import make_request_row


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

if __name__ == "__main__":
    unittest.main()
