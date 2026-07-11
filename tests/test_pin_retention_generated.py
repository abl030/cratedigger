#!/usr/bin/env python3
"""Pinned + generated invariants for terminal media-server pin retention."""
from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st

from tests.fakes import FakePipelineDB

RETENTION_DAYS = 90
NOW = datetime(2026, 7, 11, 0, 0, tzinfo=timezone.utc)
CUTOFF = NOW - timedelta(days=RETENTION_DAYS)


@dataclass(frozen=True)
class PinRow:
    backend: str
    status: str
    age_days: int
    has_reconciled_at: bool


@st.composite
def pin_worlds(draw) -> tuple[PinRow, ...]:
    rows: list[PinRow] = []
    for _ in range(draw(st.integers(min_value=0, max_value=12))):
        backend = draw(st.sampled_from(("plex", "jellyfin")))
        statuses = (
            ("pending", "done", "skipped")
            if backend == "plex"
            else ("pending", "done", "skipped", "expired")
        )
        rows.append(PinRow(
            backend=backend,
            status=draw(st.sampled_from(statuses)),
            age_days=draw(st.one_of(
                st.integers(min_value=0, max_value=89),
                st.integers(min_value=91, max_value=500),
            )),
            has_reconciled_at=draw(st.booleans()),
        ))
    return tuple(rows)


def expected_survivors(rows: tuple[PinRow, ...]) -> tuple[PinRow, ...]:
    """A row is pruned only when terminal, timestamped, and strictly old."""
    return tuple(row for row in rows if not (
        row.status != "pending"
        and row.has_reconciled_at
        and row.age_days > RETENTION_DAYS
    ))


def assert_retention_matches_oracle(
    rows: tuple[PinRow, ...], survivors: tuple[PinRow, ...],
) -> None:
    expected = expected_survivors(rows)
    if survivors != expected:
        raise AssertionError(
            f"pin retention diverged: expected={expected!r}, actual={survivors!r}")


def _run_world(rows: tuple[PinRow, ...]) -> tuple[PinRow, ...]:
    db = FakePipelineDB()
    seeded: list[tuple[PinRow, dict[str, object]]] = []
    for index, row in enumerate(rows):
        if row.backend == "plex":
            pin_id = db.add_plex_added_at_pin(
                imported_path=f"plex-{index}", original_added_at=index,
                rating_key=None, request_id=None)
            stored = db.plex_added_at_pins[pin_id - 1]
        else:
            pin_id = db.add_jellyfin_date_created_pin(
                imported_path=f"jellyfin-{index}",
                original_date_created="2000-01-01T00:00:00Z",
                album_item_id=f"album-{index}", children_item_ids=[],
                request_id=None)
            stored = db.jellyfin_date_created_pins[pin_id - 1]
        stored["status"] = row.status
        stored["reconciled_at"] = (
            NOW - timedelta(days=row.age_days)
            if row.has_reconciled_at else None
        )
        seeded.append((row, stored))

    db.prune_terminal_plex_added_at_pins(older_than=CUTOFF)
    db.prune_terminal_jellyfin_date_created_pins(older_than=CUTOFF)
    surviving_ids = {
        id(stored)
        for stored in (*db.plex_added_at_pins, *db.jellyfin_date_created_pins)
    }
    return tuple(row for row, stored in seeded if id(stored) in surviving_ids)


class TestGeneratedPinRetention(unittest.TestCase):
    @given(rows=pin_worlds())
    def test_only_strictly_old_terminal_rows_are_pruned(self, rows):
        assert_retention_matches_oracle(rows, _run_world(rows))


class TestPinRetentionCheckerTripsOnViolation(unittest.TestCase):
    def test_checker_rejects_pruning_an_old_pending_row(self):
        old_pending = PinRow("plex", "pending", 365, True)
        with self.assertRaises(AssertionError):
            assert_retention_matches_oracle((old_pending,), ())


if __name__ == "__main__":
    unittest.main()
