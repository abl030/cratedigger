"""Self-tests for the FakePipelineDB slskd search-ledger cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import UTC, datetime, timedelta

from tests.fakes import (
    FakePipelineDB,
)


class TestFakePipelineDBSearchLedger(unittest.TestCase):
    """Self-tests for the slskd search-id write-ahead ledger stubs
    (migration 044, issue #576)."""

    def test_record_search_id_appears_in_unswept_when_old_enough(self):
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 42)
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(UTC) + timedelta(seconds=1))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["search_id"], "sid-1")
        self.assertEqual(rows[0]["purpose"], "plan_search")
        self.assertEqual(rows[0]["request_id"], 42)

    def test_get_unswept_search_ids_respects_older_than_cutoff(self):
        # A row created "now" is not yet older than a cutoff in the past —
        # mirrors the sweep's GRACE window (in-flight searches of the
        # current cycle are excluded).
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(UTC) - timedelta(hours=1))
        self.assertEqual(rows, [])

    def test_record_search_id_is_idempotent_on_conflict(self):
        # ON CONFLICT DO NOTHING: re-recording the same id is a call-
        # recording event, but the table state (and its created_at) is
        # NOT overwritten by the second call.
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        first = db._search_ledger["sid-1"].created_at
        db.record_search_id("sid-1", "artist_probe", 2)
        self.assertEqual(db._search_ledger["sid-1"].created_at, first)
        self.assertEqual(db._search_ledger["sid-1"].purpose, "plan_search")
        self.assertEqual(len(db.record_search_id_calls), 2)

    def test_mark_search_ids_deleted_removes_from_unswept(self):
        db = FakePipelineDB()
        db.record_search_id("sid-1", "plan_search", 1)
        db.record_search_id("sid-2", "plan_search", 2)
        db.mark_search_ids_deleted(["sid-1"])
        rows = db.get_unswept_search_ids(
            older_than=datetime.now(UTC) + timedelta(seconds=1))
        self.assertEqual([r["search_id"] for r in rows], ["sid-2"])

    def test_mark_search_ids_deleted_unknown_id_is_a_noop(self):
        db = FakePipelineDB()
        db.mark_search_ids_deleted(["never-recorded"])  # must not raise

    def test_prune_search_ledger_removes_only_old_deleted_rows(self):
        db = FakePipelineDB()
        db.record_search_id("sid-old", "plan_search", 1)
        db.record_search_id("sid-recent", "plan_search", 2)
        db.record_search_id("sid-undeleted", "plan_search", 3)
        db.mark_search_ids_deleted(["sid-old", "sid-recent"])
        db._search_ledger["sid-old"].deleted_at = (
            datetime.now(UTC) - timedelta(days=10))

        removed = db.prune_search_ledger(
            deleted_before=datetime.now(UTC) - timedelta(days=7))

        self.assertEqual(removed, 1)
        self.assertNotIn("sid-old", db._search_ledger)
        self.assertIn("sid-recent", db._search_ledger)
        self.assertIn("sid-undeleted", db._search_ledger)


if __name__ == "__main__":
    unittest.main()
