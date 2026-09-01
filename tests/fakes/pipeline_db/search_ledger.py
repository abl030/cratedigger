"""FakePipelineDB search_ledger cluster — mirrors ``lib/pipeline_db/search_ledger.py``.

The slskd search-id write-ahead ledger (migration 044).
"""
from __future__ import annotations

from datetime import datetime
from typing import (
    Any,
)

from tests.fakes._shared import _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.rows import (
    SearchLedgerRow,
)


class _FakeSearchLedgerMixin(_FakePipelineDBBase):
    """The slskd search-id write-ahead ledger (migration 044)."""

    def record_search_id(
        self,
        search_id: str,
        purpose: str,
        request_id: int | None,
    ) -> None:
        row = SearchLedgerRow(
            search_id=search_id, purpose=purpose, request_id=request_id)
        self.record_search_id_calls.append(row)
        # ON CONFLICT DO NOTHING: the first insert for a given search_id
        # sticks; a re-record of the same id is a call-recording event
        # only, not a table mutation.
        self._search_ledger.setdefault(search_id, row)

    def get_unswept_search_ids(self, older_than: datetime) -> list[dict[str, Any]]:
        rows = [
            r for r in self._search_ledger.values()
            if r.deleted_at is None and r.created_at < older_than
        ]
        rows.sort(key=lambda r: r.created_at)
        return [
            {
                "search_id": r.search_id,
                "created_at": r.created_at,
                "purpose": r.purpose,
                "request_id": r.request_id,
            }
            for r in rows
        ]

    def mark_search_ids_deleted(self, search_ids: list[str]) -> None:
        now = _utcnow()
        for search_id in search_ids:
            row = self._search_ledger.get(search_id)
            if row is not None:
                row.deleted_at = now

    def prune_search_ledger(self, deleted_before: datetime) -> int:
        to_remove = [
            search_id for search_id, r in self._search_ledger.items()
            if r.deleted_at is not None and r.deleted_at < deleted_before
        ]
        for search_id in to_remove:
            del self._search_ledger[search_id]
        return len(to_remove)

