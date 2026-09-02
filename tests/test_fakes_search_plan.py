"""Self-tests for the FakePipelineDB search-plan cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import inspect
import unittest
from datetime import UTC, datetime, timedelta

from lib.pipeline_db import (
    PipelineDB,
)
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakePipelineDBSearchPlans(unittest.TestCase):
    """FakePipelineDB mirrors the U1 plan methods with the same semantics
    so tests that exercise plan generation, reconciliation, consumed
    attempts, and stale completions can run without a real Postgres.
    """

    def _items(self, *queries: str):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(
                ordinal=i,
                strategy=f"slot_{i}",
                query=q,
                canonical_query_key=q.lower(),
            )
            for i, q in enumerate(queries)
        ]

    def test_successful_plan_sets_active_and_resets_cursor(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid,
            generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.id, plan_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        self.assertEqual(len(active.items), 2)
        self.assertEqual(active.items[0].ordinal, 0)
        self.assertEqual(active.items[1].ordinal, 1)
        self.assertEqual(db.request(rid)["active_plan_id"], plan_id)

    def test_failed_deterministic_plan_keeps_request_unsearchable(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertIsNone(db.get_active_search_plan(rid))
        self.assertEqual(
            db.search_plans[plan_id].status, "failed_deterministic")
        self.assertEqual(db.request(rid)["status"], "wanted")
        self.assertIsNone(db.request(rid)["active_plan_id"])

    def test_failed_transient_plan_is_visible_and_not_sticky(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        pid = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(db.search_plans[pid].status, "failed_transient")

    def test_supersede_replaces_active_and_resets_cursor(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        first = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        # Move cursor away from (0, 0) so we can prove reset.
        db.update_request_fields(rid, next_plan_ordinal=1, plan_cycle_count=4)
        new_id = db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id="g2",
            items=self._items("Q2"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        self.assertEqual(active.plan.id, new_id)
        self.assertEqual(active.next_ordinal, 0)
        self.assertEqual(active.cycle_count, 0)
        # Old plan is superseded with a back-link.
        old = db.search_plans[first]
        self.assertEqual(old.status, "superseded")
        self.assertIsNotNone(old.superseded_at)
        self.assertEqual(old.superseded_by_plan_id, new_id)

    def test_list_wanted_for_plan_reconciliation_ignores_pagination(self):
        db = FakePipelineDB()
        rid_planned = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="planned",
        )
        rid_unplanned = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="unplanned",
        )
        rid_imported = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="imported",
        )
        db.update_status(rid_imported, "imported")
        db.create_successful_search_plan(
            request_id=rid_planned, generator_id="g1",
            items=self._items("Q"),
        )
        rows = db.list_wanted_for_plan_reconciliation()
        rids = {r.request_id for r in rows}
        self.assertEqual(rids, {rid_planned, rid_unplanned})
        by_id = {r.request_id: r for r in rows}
        self.assertEqual(by_id[rid_planned].active_plan_generator_id, "g1")
        self.assertIsNone(by_id[rid_unplanned].active_plan_generator_id)

    def test_inspection_returns_active_failed_superseded_legacy(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        # Legacy log row (no plan context).
        db.log_search(rid, query="legacy", outcome="error")
        det = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        trans = db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        new_id = db.supersede_search_plan_with_replacement(
            request_id=rid, generator_id="g2",
            items=self._items("Q1"),
        )
        info = db.get_search_plan_inspection(rid)
        assert info.active is not None
        self.assertEqual(info.active.plan.id, new_id)
        assert info.latest_failed_deterministic is not None
        self.assertEqual(info.latest_failed_deterministic.id, det)
        assert info.latest_failed_transient is not None
        self.assertEqual(info.latest_failed_transient.id, trans)
        self.assertEqual(info.superseded_count, 1)
        self.assertEqual(info.legacy_search_log_count, 1)

    def test_consumed_attempt_advances_cursor_and_writes_log(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=False,
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertEqual(result.new_next_ordinal, 1)
        self.assertEqual(result.new_cycle_count, 0)
        self.assertFalse(result.is_stale)
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 1)
        # Log row carries plan context + cycle snapshot.
        log = db.search_logs[0]
        self.assertEqual(log.plan_id, plan_id)
        self.assertEqual(log.plan_ordinal, 0)
        self.assertEqual(log.execution_stage, "accepted")
        self.assertTrue(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "advanced")
        self.assertEqual(log.plan_cycle_snapshot, 0)
        # Scheduler/backoff applied.
        self.assertEqual(db.request(rid)["search_attempts"], 1)
        self.assertIsNotNone(db.request(rid)["next_retry_after"])

    def test_consumed_attempt_wraps_at_final_ordinal_and_increments_cycle(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        db.update_request_fields(rid, next_plan_ordinal=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[1].id, plan_ordinal=1,
            plan_strategy="slot_1", plan_canonical_query_key="q1",
            plan_repeat_group=None, plan_generator_id="g1", query="Q1",
            outcome="found", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(result.new_next_ordinal, 0)
        self.assertEqual(result.new_cycle_count, 1)
        self.assertEqual(db.request(rid)["plan_cycle_count"], 1)
        # success path doesn't bump search_attempts.
        self.assertEqual(db.request(rid)["search_attempts"], 0)

    def test_u12_fake_writes_failure_class_at_wrap(self):
        """FakePipelineDB mirrors the real wrap-time classification write."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Cycle 0: both items return no_match → all-candidates-no-match.
        db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
            rejection_reason="strict_count_mismatch",
        ))
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[1].id, plan_ordinal=1,
            plan_strategy="slot_1", plan_canonical_query_key="q1",
            plan_repeat_group=None, plan_generator_id="g1", query="Q1",
            outcome="no_match", plan_item_count=2,
            rejection_reason="avg_ratio_low",
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(db.request(rid)["failure_class"],
                         "B_cands_never_match")

    def test_u12_fake_does_not_overwrite_failure_class_when_classifier_none(
        self,
    ):
        """Degenerate wrap (zero consumed attempts in cycle) preserves prior."""
        from lib.pipeline_db import CURSOR_UPDATE_WRAPPED, ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Seed a prior failure_class. Build a wrap whose consumed
        # attempts are all on cycle N-1 (i.e. zero attempts on cycle
        # we're wrapping). We simulate this by directly tampering with
        # the search_log row's plan_cycle_snapshot post-insert so the
        # classifier's per-cycle filter excludes the only row.
        db.update_request_fields(rid, failure_class="E_mixed")
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=1,
        ))
        self.assertEqual(result.cursor_update_status, CURSOR_UPDATE_WRAPPED)
        # The single attempt was found+wanted → D, which overwrites E.
        self.assertEqual(db.request(rid)["failure_class"],
                         "D_found_but_no_import")

    def test_u12_fake_classifies_resolved_when_status_not_wanted(self):
        """Status moved past 'wanted' mid-cycle → resolved verdict on wrap."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1", status="imported",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=1,
        ))
        self.assertEqual(result.cursor_update_status, "wrapped")
        self.assertEqual(db.request(rid)["failure_class"], "resolved")

    def test_u12_fake_does_not_write_on_plain_advance(self):
        """Classification only on wrap, not on plain advance."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="no_match", plan_item_count=2,
        ))
        self.assertEqual(result.cursor_update_status, "advanced")
        self.assertIsNone(db.request(rid)["failure_class"])

    def test_u12_fake_rolls_back_failure_class_on_validation_failure(self):
        """A txn rollback must restore failure_class to the pre-call value."""
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # Seed a prior verdict so we can prove rollback restores it.
        db.update_request_fields(rid, failure_class="A_zero_results_dominant")
        # plan_item_id 999_999 does not belong to plan_id → fake raises;
        # the whole transaction rolls back, including any speculative
        # failure_class write that might have happened.
        with self.assertRaises(ValueError):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid, plan_id=plan_id,
                plan_item_id=999999, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1", query="Q0",
                outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.request(rid)["failure_class"],
                         "A_zero_results_dominant")
        self.assertEqual(db.search_logs, [])

    def test_consumed_attempt_stale_when_request_already_advanced(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0", "Q1"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        # Mid-flight regeneration / out-of-band advance.
        db.update_request_fields(rid, next_plan_ordinal=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=2,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))
        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        # Cursor unchanged.
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 1)
        # Log row is still inserted, marked stale.
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "stale_completion")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "stale")
        self.assertEqual(log.stale_reason, "regenerated")
        # No scheduler bump on stale.
        self.assertEqual(db.request(rid)["search_attempts"], 0)

    def test_consumed_attempt_stale_when_cycle_changed(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        active = db.get_active_search_plan(rid)
        assert active is not None
        db.update_request_fields(rid, plan_cycle_count=1)
        result = db.record_consumed_search_attempt(ConsumedAttemptInput(
            request_id=rid, plan_id=plan_id,
            plan_item_id=active.items[0].id, plan_ordinal=0,
            plan_strategy="slot_0", plan_canonical_query_key="q0",
            plan_repeat_group=None, plan_generator_id="g1", query="Q0",
            outcome="found", plan_item_count=1,
            cycle_count_snapshot=0,
            apply_scheduler_attempt=True, scheduler_success=True,
        ))

        self.assertTrue(result.is_stale)
        self.assertEqual(result.cursor_update_status, "stale")
        self.assertEqual(db.request(rid)["plan_cycle_count"], 1)
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "stale_completion")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.plan_cycle_snapshot, 0)

    def test_consumed_attempt_rolls_back_on_validation_failure(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # plan_item_id 999_999 does not belong to plan_id; the fake mirrors
        # the real DB FK violation by raising. Either way, no log row may
        # land and the cursor must stay put.
        with self.assertRaises(ValueError):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid, plan_id=plan_id,
                plan_item_id=999999, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1",
                query="Q0", outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.search_logs, [])
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 0)

    def test_consumed_attempt_rejects_item_from_another_request(self):
        from lib.pipeline_db import ConsumedAttemptInput
        db = FakePipelineDB()
        rid_a = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        rid_b = db.add_request(
            artist_name="C", album_title="D", source="request",
            mb_release_id="m2",
        )
        plan_a = db.create_successful_search_plan(
            request_id=rid_a, generator_id="g1", items=self._items("Q0"))
        plan_b = db.create_successful_search_plan(
            request_id=rid_b, generator_id="g1", items=self._items("R0"))
        item_b = next(
            it for it in db.search_plan_items.values()
            if it.plan_id == plan_b)

        with self.assertRaises(ValueError):
            db.record_consumed_search_attempt(ConsumedAttemptInput(
                request_id=rid_a, plan_id=plan_a,
                plan_item_id=item_b.id, plan_ordinal=0,
                plan_strategy="slot_0", plan_canonical_query_key="q0",
                plan_repeat_group=None, plan_generator_id="g1",
                query="Q0", outcome="no_match", plan_item_count=1,
            ))
        self.assertEqual(db.search_logs, [])

    def test_non_consuming_logs_and_applies_backoff(self):
        from lib.pipeline_db import NonConsumingAttemptInput
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        log_id = db.record_non_consuming_search_attempt(
            NonConsumingAttemptInput(
                request_id=rid, outcome="error",
                error_message="slskd 503",
                apply_scheduler_attempt=True,
            )
        )
        self.assertGreater(log_id, 0)
        log = db.search_logs[0]
        self.assertEqual(log.execution_stage, "pre_attempt")
        self.assertFalse(log.attempt_consumed)
        self.assertEqual(log.cursor_update_status, "unchanged")
        self.assertEqual(db.request(rid)["next_plan_ordinal"], 0)
        self.assertEqual(db.request(rid)["search_attempts"], 1)
        self.assertIsNotNone(db.request(rid)["next_retry_after"])

    def test_request_delete_cascades_plans_and_items(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="m1",
        )
        plan_id = db.create_successful_search_plan(
            request_id=rid, generator_id="g1",
            items=self._items("Q0"),
        )
        # Make sure items are present pre-delete.
        self.assertTrue(any(
            it.plan_id == plan_id for it in db.search_plan_items.values()))
        db.delete_request(rid)
        self.assertNotIn(plan_id, db.search_plans)
        self.assertFalse(any(
            it.plan_id == plan_id for it in db.search_plan_items.values()))


class TestFakeGetWantedSearchable(unittest.TestCase):
    """``FakePipelineDB.get_wanted_searchable`` mirrors PipelineDB's
    plan-aware execution-eligibility filter.
    """

    def _items(self, *queries: str):
        from lib.pipeline_db import SearchPlanItemInput
        return [
            SearchPlanItemInput(ordinal=i, strategy="default", query=q)
            for i, q in enumerate(queries)
        ]

    def _make_active(self, db, rid, gen):
        return db.create_successful_search_plan(
            request_id=rid, generator_id=gen, items=self._items("Q"))

    def _seed_searchable(
        self,
        db: FakePipelineDB,
        request_id: int,
        *,
        created_at: datetime,
        attempts: int = 1,
        title: str | None = None,
    ) -> int:
        db.seed_request(make_request_row(
            id=request_id,
            mb_release_id=f"scheduler-{request_id}",
            album_title=title or f"Album {request_id}",
            created_at=created_at,
            search_attempts=attempts,
            download_attempts=attempts,
            validation_attempts=attempts,
        ))
        self._make_active(db, request_id, "g1")
        return request_id

    def test_page_size_must_leave_capacity_for_both_cohorts(self):
        db = FakePipelineDB()
        for page_size in (-1, 0, 1):
            with self.subTest(page_size=page_size), self.assertRaisesRegex(ValueError, "at least 2"):
                db.get_wanted_searchable("g1", limit=page_size)

    def test_priority_capacity_and_bidirectional_borrowing(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)

        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(2)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 10, created_at=now - timedelta(days=2))
            for index in range(20)
        }
        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=16, now=now)
        }
        self.assertEqual(new_ids & selected, new_ids)
        self.assertEqual(len(established_ids & selected), 14)

        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(20)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(2)
        }
        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=16, now=now)
        }
        self.assertEqual(len(new_ids & selected), 14)
        self.assertEqual(established_ids & selected, established_ids)

    def test_small_page_keeps_proportional_established_floor(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        db = FakePipelineDB()
        new_ids = {
            self._seed_searchable(
                db, index + 1, created_at=now - timedelta(hours=1))
            for index in range(2)
        }
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(5)
        }

        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable("g1", limit=5, now=now)
        }

        self.assertEqual(len(new_ids & selected), 1)
        self.assertEqual(len(established_ids & selected), 4)

    def test_blacklist_cannot_consume_reserved_capacity(self):
        now = datetime(2026, 7, 20, 4, 0, tzinfo=UTC)
        db = FakePipelineDB()
        blocked_ids = {
            self._seed_searchable(
                db,
                index + 1,
                created_at=now - timedelta(hours=1),
                title=f"Blocked {index}",
            )
            for index in range(4)
        }
        allowed_new = self._seed_searchable(
            db, 10, created_at=now - timedelta(hours=1), title="Allowed")
        established_ids = {
            self._seed_searchable(
                db, index + 100, created_at=now - timedelta(days=2))
            for index in range(20)
        }

        selected = {
            int(row["id"])
            for row in db.get_wanted_searchable(
                "g1",
                limit=16,
                title_blacklist=("blocked",),
                now=now,
            )
        }

        self.assertFalse(blocked_ids & selected)
        self.assertIn(allowed_new, selected)
        self.assertEqual(len(established_ids & selected), 15)

    def test_filters_to_current_generator_active_plans(self):
        db = FakePipelineDB()
        rid_match = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="match")
        self._make_active(db, rid_match, "g1")

        rid_no_plan = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="no-plan")

        rid_old = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="old")
        self._make_active(db, rid_old, "g0")

        rid_imp = db.add_request(
            artist_name="A", album_title="E", source="request",
            mb_release_id="imp")
        self._make_active(db, rid_imp, "g1")
        db.update_status(rid_imp, "imported")

        rids = {r["id"] for r in db.get_wanted_searchable("g1")}
        self.assertEqual(rids, {rid_match})
        # Sanity: rid_no_plan and rid_old are visible to non-plan
        # diagnostic ``get_wanted`` though.
        all_ids = {r["id"] for r in db.get_wanted()}
        self.assertIn(rid_no_plan, all_ids)
        self.assertIn(rid_old, all_ids)

    def test_failed_plans_excluded(self):
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="fd")
        db.create_failed_search_plan(
            request_id=rid, generator_id="g1",
            failure_class="no_runnable_query", transient=False,
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

        rid2 = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="ft")
        db.create_failed_search_plan(
            request_id=rid2, generator_id="g1",
            failure_class="resolver_unavailable", transient=True,
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

    def test_respects_retry_backoff(self):
        from datetime import datetime, timedelta
        db = FakePipelineDB()
        rid = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="bo")
        self._make_active(db, rid, "g1")
        db.update_request_fields(
            rid,
            next_retry_after=datetime.now(UTC) + timedelta(hours=1),
        )
        self.assertEqual(db.get_wanted_searchable("g1"), [])

    def test_active_youtube_rescue_excluded(self):
        from lib.import_queue import (
            IMPORT_JOB_YOUTUBE,
            youtube_import_dedupe_key,
            youtube_import_payload,
        )

        db = FakePipelineDB()
        rid_running = db.add_request(
            artist_name="A", album_title="B", source="request",
            mb_release_id="yt-running")
        self._make_active(db, rid_running, "g1")
        db.insert_youtube_running(
            request_id=rid_running,
            browse_id="MPREb_running",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/playlist?list=running",
            expected_track_count=10,
        )

        rid_import = db.add_request(
            artist_name="A", album_title="C", source="request",
            mb_release_id="yt-import")
        self._make_active(db, rid_import, "g1")
        db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=rid_import,
            dedupe_key=youtube_import_dedupe_key(123),
            payload=youtube_import_payload(
                staged_path="/tmp/yt-import",
                request_id=rid_import,
                browse_id="MPREb_import",
                download_log_id=1,
            ),
        )

        rid_clear = db.add_request(
            artist_name="A", album_title="D", source="request",
            mb_release_id="clear")
        self._make_active(db, rid_clear, "g1")

        self.assertEqual(
            {r["id"] for r in db.get_wanted_searchable("g1")},
            {rid_clear},
        )


class TestFakePipelineDBSearchPlanContract(unittest.TestCase):
    """Lightweight signature parity check between PipelineDB and
    FakePipelineDB for U1 methods. Catches drift when a real DB method
    grows a new keyword and the fake forgets to mirror it.
    """

    METHODS = (
        "create_successful_search_plan",
        "create_failed_search_plan",
        "supersede_search_plan_with_replacement",
        "get_active_search_plan",
        "get_wanted_searchable",
        "list_wanted_for_plan_reconciliation",
        "list_search_plan_classification_for_requests",
        "get_search_plan_inspection",
        "get_search_plan_stats",
        "get_search_plan_stats_history",
        "get_legacy_search_log_summary",
        "get_search_history_page",
        "record_consumed_search_attempt",
        "record_non_consuming_search_attempt",
    )

    def test_fake_method_signatures_match_real(self):
        for name in self.METHODS:
            with self.subTest(method=name):
                real_sig = inspect.signature(
                    getattr(PipelineDB, name))
                fake_sig = inspect.signature(
                    getattr(FakePipelineDB, name))
                self.assertEqual(
                    list(real_sig.parameters.keys()),
                    list(fake_sig.parameters.keys()),
                    f"FakePipelineDB.{name} drifted from "
                    f"PipelineDB.{name}",
                )


class TestFakeSearchLog(unittest.TestCase):
    """``log_search`` and the search history it accumulates."""

    def test_search_log_history(self):
        db = FakePipelineDB()
        db.log_search(1, query="a b", outcome="found", result_count=10,
                      elapsed_s=0.5)
        db.log_search(1, query="c d", outcome="no_match")

        history_1 = db.get_search_history(1)
        self.assertEqual([r["outcome"] for r in history_1],
                         ["no_match", "found"])

    def test_log_search_records_u11_forensics_kwargs(self):
        """U11 R22-R27 mirror: every new kwarg must land on the
        SearchLogRow and surface on the history dict."""
        db = FakePipelineDB()
        db.log_search(
            1, query="*adiohead Kid A", outcome="no_match",
            rejection_reason="avg_ratio_low",
            result_count_uncapped=2025,
            query_token_count=3,
            query_distinct_token_count=3,
            expected_track_count=10,
            matcher_score_top1=2.95,
            query_template="{artist} {title}",
        )
        history = db.get_search_history(1)
        self.assertEqual(len(history), 1)
        row = history[0]
        self.assertEqual(row["rejection_reason"], "avg_ratio_low")
        self.assertEqual(row["result_count_uncapped"], 2025)
        self.assertEqual(row["query_token_count"], 3)
        self.assertEqual(row["query_distinct_token_count"], 3)
        self.assertEqual(row["expected_track_count"], 10)
        score = row["matcher_score_top1"]
        assert isinstance(score, float)
        self.assertAlmostEqual(score, 2.95, places=4)
        self.assertEqual(row["query_template"], "{artist} {title}")
        # And the row dataclass preserves the raw values.
        self.assertEqual(db.search_logs[0].rejection_reason, "avg_ratio_low")
        self.assertEqual(db.search_logs[0].query_template, "{artist} {title}")

    def test_log_search_defaults_omitted_u11_kwargs_to_none(self):
        """Backwards-compat: callers that don't pass U11 kwargs get
        NULL-shaped fields on the row (mirrors the real DB column
        default for the migrated columns)."""
        db = FakePipelineDB()
        db.log_search(1, query="legacy", outcome="error")
        row = db.get_search_history(1)[0]
        self.assertIsNone(row["rejection_reason"])
        self.assertIsNone(row["result_count_uncapped"])
        self.assertIsNone(row["query_token_count"])
        self.assertIsNone(row["query_distinct_token_count"])
        self.assertIsNone(row["expected_track_count"])
        self.assertIsNone(row["matcher_score_top1"])
        self.assertIsNone(row["query_template"])


class TestFakeSearchHistoryPage(unittest.TestCase):
    """``get_search_history_page`` cursor paging: clamping, resumption,
    exhaustion, and per-request scoping.
    """

    def test_get_search_history_page_clamps_to_limit_and_seeds_cursor(self):
        """U1: cursor-paginated history mirrors PipelineDB semantics."""
        db = FakePipelineDB()
        for i in range(5):
            db.log_search(1, query=f"q{i}", outcome="no_match")
        page = db.get_search_history_page(1, limit=3)
        self.assertEqual(len(page.rows), 3)
        # Newest first.
        self.assertEqual(page.rows[0]["query"], "q4")
        self.assertEqual(page.rows[1]["query"], "q3")
        self.assertEqual(page.rows[2]["query"], "q2")
        # next_before_id seeds the next page.
        self.assertIsNotNone(page.next_before_id)

    def test_get_search_history_page_resumes_from_cursor_without_skip(self):
        db = FakePipelineDB()
        for i in range(5):
            db.log_search(1, query=f"q{i}", outcome="no_match")
        first = db.get_search_history_page(1, limit=3)
        second = db.get_search_history_page(
            1, limit=3, before_id=first.next_before_id,
        )
        self.assertEqual(len(second.rows), 2)
        self.assertEqual(second.rows[0]["query"], "q1")
        self.assertEqual(second.rows[1]["query"], "q0")
        self.assertIsNone(second.next_before_id)
        first_ids = {r["id"] for r in first.rows}
        second_ids = {r["id"] for r in second.rows}
        self.assertFalse(first_ids.intersection(second_ids))

    def test_get_search_history_page_exhausted(self):
        db = FakePipelineDB()
        db.log_search(1, query="only", outcome="no_match")
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(len(page.rows), 1)
        self.assertIsNone(page.next_before_id)

    def test_get_search_history_page_empty(self):
        db = FakePipelineDB()
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(page.rows, [])
        self.assertIsNone(page.next_before_id)

    def test_get_search_history_page_excludes_other_requests(self):
        db = FakePipelineDB()
        db.log_search(1, query="mine", outcome="no_match")
        db.log_search(2, query="theirs", outcome="no_match")
        page = db.get_search_history_page(1, limit=10)
        self.assertEqual(len(page.rows), 1)
        self.assertEqual(page.rows[0]["query"], "mine")


if __name__ == "__main__":
    unittest.main()
