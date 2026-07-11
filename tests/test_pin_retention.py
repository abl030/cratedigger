"""Orchestration pins for terminal media-server pin retention."""
from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from lib.pin_retention import PIN_RETENTION_DAYS, prune_terminal_pin_rows_cycle
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db


class TestPruneTerminalPinRowsCycle(unittest.TestCase):
    def test_one_phase_zero_step_prunes_both_backends(self):
        db = FakePipelineDB()
        now = datetime(2026, 7, 11, 12, 0, tzinfo=timezone.utc)
        old = now - timedelta(days=PIN_RETENTION_DAYS, seconds=1)

        plex_id = db.add_plex_added_at_pin(
            imported_path="plex", original_added_at=1,
            rating_key=None, request_id=None)
        db.plex_added_at_pins[plex_id - 1].update(
            status="done", reconciled_at=old)
        jellyfin_id = db.add_jellyfin_date_created_pin(
            imported_path="jellyfin",
            original_date_created="2000-01-01T00:00:00Z",
            album_item_id="album", children_item_ids=[], request_id=None)
        db.jellyfin_date_created_pins[jellyfin_id - 1].update(
            status="expired", reconciled_at=old)

        removed = prune_terminal_pin_rows_cycle(
            make_ctx_with_fake_db(db), now=now)

        self.assertEqual(removed, 2)
        self.assertEqual(db.plex_added_at_pins, [])
        self.assertEqual(db.jellyfin_date_created_pins, [])


if __name__ == "__main__":
    unittest.main()
