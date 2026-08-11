"""Canonical pipeline-delete authority and overlap tests."""

from __future__ import annotations

import copy
import unittest
from concurrent.futures import ThreadPoolExecutor

from lib import transitions
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT, PipelineDB
from lib.pipeline_delete_service import (
    PipelineDeleteApplied,
    PipelineDeleteDescendantConflict,
    PipelineDeleteLockContended,
    delete_pipeline_request,
)
from tests.fakes import FakePipelineDB
from tests.helpers import handoff_automation_owner, make_request_row
from tests.test_pipeline_db import TEST_DSN, make_db, requires_postgres


class TestPipelineDeleteService(unittest.TestCase):
    def test_processing_owner_is_an_exact_zero_effect_conflict(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="wanted"))
        owner = handoff_automation_owner(db, 41)
        before_request = copy.deepcopy(db.get_request(41))
        before_jobs = copy.deepcopy(db.list_import_jobs())

        result = delete_pipeline_request(db, 41)

        self.assertIsInstance(result, transitions.TransitionConflict)
        assert isinstance(result, transitions.TransitionConflict)
        self.assertEqual(
            result.kind,
            transitions.TransitionConflictKind.processing_locked,
        )
        self.assertIsNotNone(result.processing_owner)
        assert result.processing_owner is not None
        self.assertEqual(result.processing_owner.job_id, owner.id)
        self.assertEqual(db.get_request(41), before_request)
        self.assertEqual(db.list_import_jobs(), before_jobs)

    def test_unowned_request_deletes_through_conditional_command(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="imported"))

        result = delete_pipeline_request(db, 41)

        self.assertEqual(result, PipelineDeleteApplied(41))
        self.assertIsNone(db.get_request(41))

    def test_descendant_conflict_preserves_the_complete_chain(self) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(id=41, status="replaced"))
        db.seed_request(make_request_row(
            id=42,
            status="replaced",
            replaces_request_id=41,
        ))
        db.seed_request(make_request_row(
            id=43,
            status="wanted",
            replaces_request_id=42,
        ))

        result = delete_pipeline_request(db, 41)

        self.assertEqual(
            result,
            PipelineDeleteDescendantConflict(41, (42, 43)),
        )
        self.assertIsNotNone(db.get_request(41))


@requires_postgres
class TestPipelineDeleteServiceRealPostgres(unittest.TestCase):
    def test_two_sessions_order_on_import_then_reread_exact_owner(self) -> None:
        db1 = make_db()
        request_id = db1.add_request(
            "Owned",
            "Delete overlap",
            "request",
            mb_release_id="pipeline-delete-overlap",
            status="wanted",
        )
        owner = handoff_automation_owner(db1, request_id)
        try:
            with db1.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                request_id,
            ) as acquired:
                self.assertTrue(acquired)

                def contend() -> object:
                    assert TEST_DSN is not None
                    db2 = PipelineDB(TEST_DSN)
                    try:
                        return delete_pipeline_request(db2, request_id)
                    finally:
                        db2.close()

                with ThreadPoolExecutor(max_workers=1) as pool:
                    contended = pool.submit(contend).result(timeout=5)
            self.assertEqual(
                contended,
                PipelineDeleteLockContended(request_id),
            )

            assert TEST_DSN is not None
            db2 = PipelineDB(TEST_DSN)
            try:
                result = delete_pipeline_request(db2, request_id)
            finally:
                db2.close()
            self.assertIsInstance(result, transitions.TransitionConflict)
            assert isinstance(result, transitions.TransitionConflict)
            self.assertEqual(
                result.kind,
                transitions.TransitionConflictKind.processing_locked,
            )
            self.assertIsNotNone(result.processing_owner)
            assert result.processing_owner is not None
            self.assertEqual(result.processing_owner.job_id, owner.id)
            current = db1.get_request(request_id)
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(
                current["active_automation_import_job_id"],
                owner.id,
            )
            self.assertIsNotNone(db1.get_import_job(owner.id))
        finally:
            db1.close()


if __name__ == "__main__":
    unittest.main()
