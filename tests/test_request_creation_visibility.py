"""Real-PostgreSQL visibility pin for issue #791's provisional status."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.dirname(__file__))

import conftest  # noqa: F401 -- starts the ephemeral PostgreSQL fixture

from lib import transitions
from lib.config import CratediggerConfig
from lib.pipeline_db import PipelineDB
from lib.search import SEARCH_PLAN_GENERATOR_ID
from lib.search_plan_service import SearchPlanService


TEST_DSN = os.environ.get("TEST_DB_DSN")


@unittest.skipUnless(TEST_DSN, "ephemeral PostgreSQL unavailable")
class TestRequestCreationVisibility(unittest.TestCase):
    def setUp(self) -> None:
        assert TEST_DSN is not None
        self.writer = PipelineDB(TEST_DSN)
        self.observer = PipelineDB(TEST_DSN)
        for table in ("search_log", "search_plan_items", "search_plans", "album_tracks", "album_requests"):
            self.writer._execute(f"TRUNCATE {table} CASCADE")
        self.writer.conn.commit()

    def tearDown(self) -> None:
        self.writer.close()
        self.observer.close()

    def test_other_connection_cannot_select_initialized_plan_before_publication(self) -> None:
        request_id = self.writer.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="791-visibility", status="initializing",
        )
        tracks = [{"disc_number": 1, "track_number": 1, "title": "One"}]
        self.writer.set_tracks(request_id, tracks)
        plan = SearchPlanService(self.writer, CratediggerConfig()).generate_for_new_request(
            request_id, artist_name="A", album_title="B", year=None, tracks=tracks,
        )
        self.assertIsNotNone(plan.plan_id)
        before = self.observer.get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID)
        self.assertNotIn(request_id, {row["id"] for row in before})
        result = transitions.finalize_request(
            self.writer, request_id,
            transitions.RequestTransition.to_wanted(from_status="initializing"),
        )
        self.assertIsInstance(result, transitions.TransitionApplied)
        after = self.observer.get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID)
        self.assertIn(request_id, {row["id"] for row in after})
