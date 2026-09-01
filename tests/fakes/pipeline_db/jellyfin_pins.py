"""FakePipelineDB jellyfin_pins cluster — mirrors ``lib/pipeline_db/jellyfin_pins.py``.

``jellyfin_date_created_pins`` (migration 046).
"""
from __future__ import annotations

import copy
from datetime import datetime
from typing import (
    Any,
)

from lib.pipeline_db import (
    JELLYFIN_PIN_STATUSES,
    JELLYFIN_TERMINAL_PIN_STATUSES,
    JellyfinTerminalPinStatus,
)
from tests.fakes._shared import _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase


class _FakeJellyfinPinsMixin(_FakePipelineDBBase):
    """``jellyfin_date_created_pins`` (migration 046)."""

    def add_jellyfin_date_created_pin(
        self,
        *,
        imported_path: str,
        original_date_created: str,
        album_item_id: str | None,
        children_item_ids: list[str],
        request_id: int | None,
    ) -> int:
        self._next_jellyfin_pin_id += 1
        pin_id = self._next_jellyfin_pin_id
        self.jellyfin_date_created_pins.append({
            "id": pin_id,
            "request_id": request_id,
            "imported_path": imported_path,
            "original_date_created": original_date_created,
            "album_item_id": album_item_id,
            "children_item_ids": list(children_item_ids),
            "status": "pending",
            "captured_at": _utcnow(),
            "reconciled_at": None,
        })
        return pin_id

    def get_pending_jellyfin_date_created_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        rows = [
            copy.deepcopy(p) for p in self.jellyfin_date_created_pins
            if p["status"] == "pending"
            and _as_datetime(p["captured_at"]) < captured_before
        ]
        rows.sort(key=lambda p: (_as_datetime(p["captured_at"]), p["id"]))
        return rows[:limit]

    def mark_jellyfin_date_created_pin(
        self,
        pin_id: int,
        *,
        status: JellyfinTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        for p in self.jellyfin_date_created_pins:
            if p["id"] == pin_id:
                if status not in JELLYFIN_PIN_STATUSES:
                    import psycopg2.errors

                    raise psycopg2.errors.CheckViolation(
                        "new row for relation \"jellyfin_date_created_pins\" "
                        "violates check constraint "
                        "\"jellyfin_date_created_pins_status_check\""
                    )
                p["status"] = status
                p["reconciled_at"] = reconciled_at
                return

    def prune_terminal_jellyfin_date_created_pins(
        self,
        *,
        older_than: datetime,
    ) -> int:
        survivors = [
            p for p in self.jellyfin_date_created_pins
            if not (
                p["status"] in JELLYFIN_TERMINAL_PIN_STATUSES
                and p["reconciled_at"] is not None
                and _as_datetime(p["reconciled_at"]) < older_than
            )
        ]
        removed = len(self.jellyfin_date_created_pins) - len(survivors)
        self.jellyfin_date_created_pins[:] = survivors
        return removed

