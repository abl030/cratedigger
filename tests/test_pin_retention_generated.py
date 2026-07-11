#!/usr/bin/env python3
"""Pinned + generated invariants for terminal media-server pin retention."""
from __future__ import annotations

import copy
import os
import sys
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401
from hypothesis import given
from hypothesis import strategies as st
import psycopg2.errors

from lib.pipeline_db import (
    JELLYFIN_PIN_STATUSES,
    JELLYFIN_TERMINAL_PIN_STATUSES,
    PLEX_PIN_STATUSES,
    PLEX_TERMINAL_PIN_STATUSES,
)
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
            tuple(sorted(PLEX_PIN_STATUSES))
            if backend == "plex"
            else tuple(sorted(JELLYFIN_PIN_STATUSES))
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


def assert_status_write_matches_domain(
    *,
    backend: str,
    status: str,
    before: dict[str, object],
    after: dict[str, object],
    error: Exception | None,
) -> None:
    """A status write succeeds exactly inside its backend's closed domain."""
    domain = PLEX_PIN_STATUSES if backend == "plex" else JELLYFIN_PIN_STATUSES
    if status in domain:
        if error is not None or after["status"] != status:
            raise AssertionError(
                f"valid {backend} status {status!r} was not persisted")
        return
    if not isinstance(error, psycopg2.errors.CheckViolation):
        raise AssertionError(
            f"invalid {backend} status {status!r} did not raise CheckViolation")
    if after != before:
        raise AssertionError(
            f"invalid {backend} status {status!r} mutated the row")


def assert_status_domain_partition(
    *,
    backend: str,
    full: frozenset[str],
    terminal: frozenset[str],
) -> None:
    """The full domain is exactly live pending plus prunable terminals."""
    if not terminal.isdisjoint({"pending"}):
        raise AssertionError(f"{backend} terminal domain contains pending")
    expected = frozenset({"pending"}) | terminal
    if full != expected:
        raise AssertionError(
            f"{backend} status partition diverged: full={full!r}, "
            f"expected={expected!r}")


@st.composite
def status_writes(draw) -> tuple[str, str]:
    backend = draw(st.sampled_from(("plex", "jellyfin")))
    domain = PLEX_PIN_STATUSES if backend == "plex" else JELLYFIN_PIN_STATUSES
    status = draw(st.one_of(
        st.sampled_from(sorted(domain)),
        st.text(min_size=0, max_size=20),
    ))
    return backend, status


def _run_status_write(backend: str, status: str) -> tuple[
    dict[str, object], dict[str, object], Exception | None,
]:
    db = FakePipelineDB()
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    if backend == "plex":
        pin_id = db.add_plex_added_at_pin(
            imported_path="A/B", original_added_at=1,
            rating_key=None, request_id=None)
        row = db.plex_added_at_pins[0]
    else:
        pin_id = db.add_jellyfin_date_created_pin(
            imported_path="A/B", original_date_created="2000-01-01T00:00:00Z",
            album_item_id="album", children_item_ids=[], request_id=None)
        row = db.jellyfin_date_created_pins[0]
    before: dict[str, object] = copy.deepcopy(row)
    error: Exception | None = None
    if backend == "plex":
        try:
            db.mark_plex_added_at_pin(
                pin_id, status=status,  # type: ignore[arg-type]
                reconciled_at=now)
        except Exception as exc:
            error = exc
    else:
        try:
            db.mark_jellyfin_date_created_pin(
                pin_id, status=status,  # type: ignore[arg-type]
                reconciled_at=now)
        except Exception as exc:
            error = exc
    return before, copy.deepcopy(row), error


class TestGeneratedPinRetention(unittest.TestCase):
    @given(rows=pin_worlds())
    def test_only_strictly_old_terminal_rows_are_pruned(self, rows):
        assert_retention_matches_oracle(rows, _run_world(rows))

    @given(write=status_writes())
    def test_status_writes_match_closed_backend_domain(self, write):
        backend, status = write
        before, after, error = _run_status_write(backend, status)
        assert_status_write_matches_domain(
            backend=backend,
            status=status,
            before=before,
            after=after,
            error=error,
        )

    @given(backend=st.sampled_from(("plex", "jellyfin")))
    def test_every_status_is_pending_or_terminal(self, backend):
        full, terminal = (
            (PLEX_PIN_STATUSES, PLEX_TERMINAL_PIN_STATUSES)
            if backend == "plex"
            else (JELLYFIN_PIN_STATUSES, JELLYFIN_TERMINAL_PIN_STATUSES)
        )
        assert_status_domain_partition(
            backend=backend, full=full, terminal=terminal)


class TestPinRetentionCheckerTripsOnViolation(unittest.TestCase):
    def test_checker_rejects_pruning_an_old_pending_row(self):
        old_pending = PinRow("plex", "pending", 365, True)
        with self.assertRaises(AssertionError):
            assert_retention_matches_oracle((old_pending,), ())

    def test_status_checker_rejects_accepting_an_unknown_value(self):
        before: dict[str, object] = {"status": "pending"}
        with self.assertRaises(AssertionError):
            assert_status_write_matches_domain(
                backend="plex",
                status="stranded",
                before=before,
                after={"status": "stranded"},
                error=None,
            )

    def test_partition_checker_rejects_a_valid_but_unprunable_status(self):
        with self.assertRaises(AssertionError):
            assert_status_domain_partition(
                backend="plex",
                full=frozenset({"pending", "done", "stranded"}),
                terminal=frozenset({"done"}),
            )


if __name__ == "__main__":
    unittest.main()
