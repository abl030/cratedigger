"""Self-tests for the FakePipelineDB Jellyfin date-created pin cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import copy
import unittest
from datetime import UTC, datetime, timedelta

from tests.fakes import (
    FakePipelineDB,
)


class TestFakeJellyfinDateCreatedPins(unittest.TestCase):
    """The Jellyfin ``DateCreated`` pin queue (migration 046)."""

    def test_jellyfin_date_created_pin_add_get_pending_and_mark(self):
        """The fake mirrors migration-046 semantics: monotonic ids, pending
        filtered by status + captured_before cutoff, mark moves it terminal."""
        db = FakePipelineDB()
        now = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_date_created="2026-04-26T18:31:04.4425337Z",
            album_item_id="alb-1",
            children_item_ids=["tr-1", "tr-2"],
            request_id=8812,
        )
        self.assertEqual(pin_id, 1)
        # Force a deterministic capture time in the past, then read pending.
        db.jellyfin_date_created_pins[0]["captured_at"] = now - timedelta(minutes=10)
        pending = db.get_pending_jellyfin_date_created_pins(
            captured_before=now, limit=100)
        self.assertEqual(len(pending), 1)
        row = pending[0]
        self.assertEqual(row["original_date_created"], "2026-04-26T18:31:04.4425337Z")
        self.assertEqual(row["album_item_id"], "alb-1")
        self.assertEqual(row["children_item_ids"], ["tr-1", "tr-2"])
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")
        # A cutoff before the capture excludes the pin (settle-window guard).
        self.assertEqual(
            db.get_pending_jellyfin_date_created_pins(
                captured_before=now - timedelta(hours=1), limit=100),
            [])
        # Marking terminal removes it from pending.
        db.mark_jellyfin_date_created_pin(pin_id, status="expired", reconciled_at=now)
        self.assertEqual(
            db.get_pending_jellyfin_date_created_pins(captured_before=now, limit=100),
            [])
        self.assertEqual(db.jellyfin_date_created_pins[0]["status"], "expired")
        self.assertEqual(db.jellyfin_date_created_pins[0]["reconciled_at"], now)

    def test_jellyfin_pin_rejects_invalid_status_without_mutating_row(self):
        import psycopg2.errors

        db = FakePipelineDB()
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B",
            original_date_created="2000-01-01T00:00:00Z",
            album_item_id="album", children_item_ids=[], request_id=None)
        before = copy.deepcopy(db.jellyfin_date_created_pins[0])
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_jellyfin_date_created_pin(
                pin_id,
                status="stranded",  # pyright: ignore[reportArgumentType]
                reconciled_at=datetime.now(UTC))
        self.assertEqual(db.jellyfin_date_created_pins[0], before)

    def test_jellyfin_pin_prune_matches_strict_terminal_age_contract(self):
        db = FakePipelineDB()
        cutoff = datetime(2026, 7, 11, tzinfo=UTC)
        for status, reconciled_at in (
            ("done", cutoff - timedelta(seconds=1)),
            ("skipped", cutoff - timedelta(days=1)),
            ("expired", cutoff),
            ("pending", cutoff - timedelta(days=365)),
        ):
            pin_id = db.add_jellyfin_date_created_pin(
                imported_path=status,
                original_date_created="2000-01-01T00:00:00Z",
                album_item_id=status, children_item_ids=[], request_id=None)
            db.jellyfin_date_created_pins[pin_id - 1].update(
                status=status, reconciled_at=reconciled_at)

        removed = db.prune_terminal_jellyfin_date_created_pins(
            older_than=cutoff)

        self.assertEqual(removed, 2)
        self.assertEqual(
            [row["status"] for row in db.jellyfin_date_created_pins],
            ["expired", "pending"],
        )


if __name__ == "__main__":
    unittest.main()
