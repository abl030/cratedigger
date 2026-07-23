"""Real-PostgreSQL visibility and RELEASE-lock pins for issue #791."""

from __future__ import annotations

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(os.path.dirname(__file__))

import conftest  # noqa: F401 -- starts the ephemeral PostgreSQL fixture

from lib.config import CratediggerConfig
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_RELEASE, PipelineDB, release_id_to_lock_key
from lib.request_creation_service import RequestCreationInput, RequestCreationService
from lib.search import SEARCH_PLAN_GENERATOR_ID


TEST_DSN = os.environ.get("TEST_DB_DSN")


class _PausePublicationDB(PipelineDB):
    pause_publication = True

    def update_status(self, request_id: int, status: str, *, expected_status: str | None = None,
                      **extra: object) -> bool:
        if self.pause_publication and status == "wanted" and expected_status == "initializing":
            return False
        return super().update_status(request_id, status, expected_status=expected_status, **extra)


def _creation() -> RequestCreationInput:
    return RequestCreationInput(
        release_id="791-visibility", mb_release_id="791-visibility",
        artist_name="A", album_title="B", source="request",
        tracks=[{"disc_number": 1, "track_number": 1, "title": "One"}],
        mb_release_payload={"artist-credit": [], "media": []},
    )


class TestRequestCreationVisibility(unittest.TestCase):
    def setUp(self) -> None:
        assert TEST_DSN is not None
        self.writer = _PausePublicationDB(TEST_DSN)
        self.observer = PipelineDB(TEST_DSN)
        for table in ("search_log", "search_plan_items", "search_plans", "album_tracks", "album_requests"):
            self.writer._execute(f"TRUNCATE {table} CASCADE")
        self.writer.conn.commit()

    def tearDown(self) -> None:
        self.writer.close()
        self.observer.close()

    def test_real_service_hides_initializing_work_and_release_lock_serializes_identity(self) -> None:
        creation = _creation()
        service = RequestCreationService(self.writer, CratediggerConfig())
        first = service.create_or_resume(creation)
        self.assertEqual(first.outcome, "initialization_failed")
        assert first.request_id is not None
        self.assertIsNotNone(self.writer.get_active_search_plan(first.request_id))
        before = self.observer.get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID)
        self.assertNotIn(first.request_id, {row["id"] for row in before})

        # A second connection holding the exact RELEASE lock makes the same
        # identity retry busy rather than creating a parallel provisional row.
        with self.observer.advisory_lock(
            ADVISORY_LOCK_NAMESPACE_RELEASE,
            release_id_to_lock_key(creation.release_id),
        ) as acquired:
            self.assertTrue(acquired)
            contended = service.create_or_resume(creation)
        self.assertEqual(contended.outcome, "busy")
        self.assertEqual(self.writer.count_by_status().get("initializing"), 1)

        self.writer.pause_publication = False
        second = service.create_or_resume(creation)
        self.assertEqual(second.outcome, "resumed")
        after = self.observer.get_wanted_searchable(SEARCH_PLAN_GENERATOR_ID)
        self.assertIn(first.request_id, {row["id"] for row in after})
