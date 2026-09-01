"""FakePipelineDB plex_pins cluster — mirrors ``lib/pipeline_db/plex_pins.py``.

``plex_added_at_pins`` (migration 040).
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import (
    Any,
)

from lib.pipeline_db import (
    PLEX_PIN_STATUSES,
    PLEX_TERMINAL_PIN_STATUSES,
    PlexTerminalPinStatus,
)
from tests.fakes._shared import _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakePlexPinsMixin(_FakePipelineDBBase):
    """``plex_added_at_pins`` (migration 040)."""

    def add_plex_added_at_pin(
        self,
        *,
        imported_path: str,
        original_added_at: int,
        rating_key: str | None,
        request_id: int | None,
    ) -> int:
        self._next_plex_pin_id += 1
        pin_id = self._next_plex_pin_id
        self.plex_added_at_pins.append({
            "id": pin_id,
            "request_id": request_id,
            "imported_path": imported_path,
            "original_added_at": int(original_added_at),
            "rating_key": rating_key,
            "status": "pending",
            "captured_at": _utcnow(),
            "reconciled_at": None,
        })
        return pin_id

    def get_pending_plex_added_at_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(p) for p in self.plex_added_at_pins
            if p["status"] == "pending"
            and _as_datetime(p["captured_at"]) < captured_before
        ]
        rows.sort(key=lambda p: (_as_datetime(p["captured_at"]), p["id"]))
        return rows[:limit]

    def mark_plex_added_at_pin(
        self,
        pin_id: int,
        *,
        status: PlexTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        for p in self.plex_added_at_pins:
            if p["id"] == pin_id:
                if status not in PLEX_PIN_STATUSES:
                    import psycopg2.errors

                    raise psycopg2.errors.CheckViolation(
                        "new row for relation \"plex_added_at_pins\" violates "
                        "check constraint \"plex_added_at_pins_status_check\""
                    )
                p["status"] = status
                p["reconciled_at"] = reconciled_at
                return

    def prune_terminal_plex_added_at_pins(
        self,
        *,
        older_than: datetime,
    ) -> int:
        survivors = [
            p for p in self.plex_added_at_pins
            if not (
                p["status"] in PLEX_TERMINAL_PIN_STATUSES
                and p["reconciled_at"] is not None
                and _as_datetime(p["reconciled_at"]) < older_than
            )
        ]
        removed = len(self.plex_added_at_pins) - len(survivors)
        self.plex_added_at_pins[:] = survivors
        return removed

