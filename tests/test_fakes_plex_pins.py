"""Self-tests for the FakePipelineDB Plex added-at pin cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import copy
import unittest
from datetime import UTC, datetime, timedelta

from tests.fakes import (
    FakePipelineDB,
)


class TestFakePlexAddedAtPins(unittest.TestCase):
    """The Plex ``addedAt`` pin queue (migration 040)."""

    def test_plex_added_at_pin_add_get_pending_and_mark(self):
        """The fake mirrors migration-040 semantics: monotonic ids, pending
        filtered by status + captured_before cutoff, mark moves it terminal."""
        db = FakePipelineDB()
        now = datetime(2026, 6, 28, 12, 0, tzinfo=UTC)
        pin_id = db.add_plex_added_at_pin(
            imported_path="Muse/2026 - The Wow! Signal",
            original_added_at=1782611948,
            rating_key="458495",
            request_id=8812,
        )
        self.assertEqual(pin_id, 1)
        # Force a deterministic capture time in the past, then read pending.
        db.plex_added_at_pins[0]["captured_at"] = now - timedelta(minutes=10)
        pending = db.get_pending_plex_added_at_pins(captured_before=now, limit=100)
        self.assertEqual(len(pending), 1)
        row = pending[0]
        self.assertEqual(row["original_added_at"], 1782611948)
        self.assertEqual(row["rating_key"], "458495")
        self.assertEqual(row["request_id"], 8812)
        self.assertEqual(row["status"], "pending")
        # A cutoff before the capture excludes the pin (settle-window guard).
        self.assertEqual(
            db.get_pending_plex_added_at_pins(
                captured_before=now - timedelta(hours=1), limit=100),
            [])
        # Marking terminal removes it from pending.
        db.mark_plex_added_at_pin(pin_id, status="done", reconciled_at=now)
        self.assertEqual(
            db.get_pending_plex_added_at_pins(captured_before=now, limit=100), [])
        self.assertEqual(db.plex_added_at_pins[0]["status"], "done")
        self.assertEqual(db.plex_added_at_pins[0]["reconciled_at"], now)

    def test_plex_pin_rejects_invalid_status_without_mutating_row(self):
        import psycopg2.errors

        db = FakePipelineDB()
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=1,
            rating_key=None, request_id=None)
        before = copy.deepcopy(db.plex_added_at_pins[0])
        with self.assertRaises(psycopg2.errors.CheckViolation):
            db.mark_plex_added_at_pin(
                pin_id,
                status="stranded",  # pyright: ignore[reportArgumentType]
                reconciled_at=datetime.now(UTC))
        self.assertEqual(db.plex_added_at_pins[0], before)

    def test_plex_pin_prune_matches_strict_terminal_age_contract(self):
        db = FakePipelineDB()
        cutoff = datetime(2026, 7, 11, tzinfo=UTC)
        for status, reconciled_at in (
            ("done", cutoff - timedelta(seconds=1)),
            ("skipped", cutoff),
            ("pending", cutoff - timedelta(days=365)),
        ):
            pin_id = db.add_plex_added_at_pin(
                imported_path=status, original_added_at=1,
                rating_key=None, request_id=None)
            db.plex_added_at_pins[pin_id - 1].update(
                status=status, reconciled_at=reconciled_at)

        removed = db.prune_terminal_plex_added_at_pins(older_than=cutoff)

        self.assertEqual(removed, 1)
        self.assertEqual(
            [row["status"] for row in db.plex_added_at_pins],
            ["skipped", "pending"],
        )


if __name__ == "__main__":
    unittest.main()
