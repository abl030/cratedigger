"""slskd search-id write-ahead ledger (issue #576).

The four CRUD methods that back ``lib.slskd_searches.converge_slskd_searches``:

* ``record_search_id`` -- write-ahead INSERT, called BEFORE the slskd
  ``POST /searches`` (I2). This is what makes the leak fix kill-proof
  (I1): a process death at ANY point after the POST still leaves a
  durable row a later cycle's sweep can act on.
* ``get_unswept_search_ids`` -- the sweep's read: rows not yet confirmed
  deleted, past the caller's GRACE cutoff.
* ``mark_search_ids_deleted`` -- the sweep's write: stamps confirmed-gone
  ids so they drop out of the next sweep's scan.
* ``prune_search_ledger`` -- keeps the table bounded by hard-deleting
  already-swept rows past a retention window.

See migration 044 for the schema and ``lib/slskd_searches.py`` for the
sweep that drives these methods.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from lib.pipeline_db._core import _PipelineDBBase


class _SearchLedgerMixin(_PipelineDBBase):
    """slskd search-id write-ahead ledger CRUD (migration 044)."""

    def record_search_id(
        self,
        search_id: str,
        purpose: str,
        request_id: int | None,
    ) -> None:
        """Write-ahead insert: call this BEFORE ``searches.search_text(...)``.

        ``ON CONFLICT DO NOTHING`` because ids are unique by construction
        (client-minted ``uuid.uuid4()``) -- a conflict here would mean the
        same id was ledgered twice, which is harmless to no-op rather than
        error on.
        """
        self._execute(
            """
            INSERT INTO slskd_search_ledger (search_id, purpose, request_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (search_id) DO NOTHING
            """,
            (search_id, purpose, request_id),
        )

    def get_unswept_search_ids(
        self,
        older_than: datetime,
    ) -> list[dict[str, Any]]:
        """Rows the sweep should consider: not yet marked deleted, and
        older than the caller's GRACE cutoff.

        The GRACE window itself is the sweep's policy
        (``lib.slskd_searches.SEARCH_LEDGER_SWEEP_GRACE_S``), not this
        method's -- ``older_than`` is taken verbatim.
        """
        cur = self._execute(
            """
            SELECT search_id, created_at, purpose, request_id
            FROM slskd_search_ledger
            WHERE deleted_at IS NULL AND created_at < %s
            ORDER BY created_at ASC
            """,
            (older_than,),
        )
        return [dict(r) for r in cur.fetchall()]

    def mark_search_ids_deleted(self, search_ids: list[str]) -> None:
        """Stamp ``deleted_at`` for ids the sweep confirmed are gone from
        slskd (either it deleted them itself, or they were already absent
        -- the fast-path delete in ``execute_search``'s finally already
        worked)."""
        if not search_ids:
            return
        self._execute(
            """
            UPDATE slskd_search_ledger
            SET deleted_at = NOW()
            WHERE search_id = ANY(%s)
            """,
            (list(search_ids),),
        )

    def prune_search_ledger(self, deleted_before: datetime) -> int:
        """Hard-delete already-swept rows older than a retention window.

        Keeps the table bounded -- without this, every search cratedigger
        ever created (order 20-40/day) would accumulate here forever, the
        exact shape of the bug this ledger fixes on slskd's side. Returns
        the number of rows removed.
        """
        cur = self._execute(
            """
            DELETE FROM slskd_search_ledger
            WHERE deleted_at IS NOT NULL AND deleted_at < %s
            """,
            (deleted_before,),
        )
        return cur.rowcount
