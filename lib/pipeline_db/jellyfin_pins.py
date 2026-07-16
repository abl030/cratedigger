"""Jellyfin 'Recently Added' DateCreated pin store (migration 046, issue #574).

When an album is upgraded (re-imported at higher quality), beets replaces the
on-disk files and the Jellyfin rescan recreates the album's Audio items with
``DateCreated`` stamped from file ctime, wrongly surfacing the album in
'Recently Added'. These methods back the capture-then-reconcile loop in
``lib/jellyfin_pin_service.py`` that clamps new Audio dates to the captured
pre-upgrade maximum. See migration 046 for the table's original rationale.
"""
import json
from datetime import datetime
from typing import Any

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db.pin_status import (
    JELLYFIN_TERMINAL_PIN_STATUSES,
    JellyfinTerminalPinStatus,
)


class _JellyfinPinsMixin(_PipelineDBBase):
    """CRUD for ``jellyfin_date_created_pins`` (migration 046)."""

    def add_jellyfin_date_created_pin(
        self,
        *,
        imported_path: str,
        original_date_created: str,
        album_item_id: str | None,
        children_item_ids: list[str],
        request_id: int | None,
    ) -> int:
        """Record a pending pin capturing an album's pre-upgrade
        maximum Audio ``DateCreated`` plus the item-id snapshot the
        reconciler compares against. ``album_item_id=None`` marks a FLOOR
        pin (migration 053): no pre-upgrade item was findable, so the
        date is the pipeline's own floor and any album that appears at
        the path counts as the landed rescan. Returns the new pin id.
        """
        cur = self._execute(
            """
            INSERT INTO jellyfin_date_created_pins
                (imported_path, original_date_created, album_item_id,
                 children_item_ids, request_id, status)
            VALUES (%s, %s, %s, %s::jsonb, %s, 'pending')
            RETURNING id
            """,
            (imported_path, original_date_created, album_item_id,
             json.dumps(list(children_item_ids)), request_id),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING should always return a row"
        return row["id"]

    def get_pending_jellyfin_date_created_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return pending pins captured before ``captured_before``, oldest
        first. The cutoff is the reconciler's settle window; the real
        "may we act yet" gate is the id/date detector in the service.
        """
        cur = self._execute(
            """
            SELECT id, request_id, imported_path, original_date_created,
                   album_item_id, children_item_ids, status, captured_at,
                   reconciled_at
            FROM jellyfin_date_created_pins
            WHERE status = 'pending' AND captured_at < %s
            ORDER BY captured_at ASC, id ASC
            LIMIT %s
            """,
            (captured_before, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]

    def mark_jellyfin_date_created_pin(
        self,
        pin_id: int,
        *,
        status: JellyfinTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        """Mark a pin terminal: ``status`` is 'done' (restored /
        already-correct), 'skipped' (album no longer locatable in Jellyfin) or
        'expired' (TTL passed with no observable rescan)."""
        self._execute(
            """
            UPDATE jellyfin_date_created_pins
            SET status = %s, reconciled_at = %s
            WHERE id = %s
            """,
            (status, reconciled_at, int(pin_id)),
        )

    def prune_terminal_jellyfin_date_created_pins(
        self,
        *,
        older_than: datetime,
    ) -> int:
        """Hard-delete terminal convergence rows strictly older than cutoff.

        Pending rows are live bookkeeping and survive regardless of age.
        ``reconciled_at == older_than`` also survives: retention uses a strict
        age boundary, matching the transfer-ledger pruner convention.
        """
        cur = self._execute(
            """
            DELETE FROM jellyfin_date_created_pins
            WHERE status = ANY(%s)
              AND reconciled_at < %s
            """,
            (list(JELLYFIN_TERMINAL_PIN_STATUSES), older_than),
        )
        return cur.rowcount
