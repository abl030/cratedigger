"""Plex 'Recently Added' addedAt pin store (migration 040).

When an album is upgraded (re-imported at higher quality), beets replaces the
on-disk files and the post-import Plex partial scan re-stamps the album's
``addedAt`` to now, wrongly surfacing it in 'Recently Added'. These three
methods back the capture-then-reconcile loop in ``lib/plex_pin_service.py``
that restores the original date. See migration 040 for the schema rationale.
"""
from datetime import datetime
from typing import Any

from lib.pipeline_db._core import _PipelineDBBase
from lib.pipeline_db.pin_status import (
    PLEX_TERMINAL_PIN_STATUSES,
    PlexTerminalPinStatus,
)


class _PlexPinsMixin(_PipelineDBBase):
    """CRUD for ``plex_added_at_pins`` (migration 040)."""

    def add_plex_added_at_pin(
        self,
        *,
        imported_path: str,
        original_added_at: int,
        rating_key: str | None,
        request_id: int | None,
    ) -> int:
        """Record a pending pin capturing an album's pre-upgrade ``addedAt``.

        ``original_added_at`` is a Unix epoch (seconds). Returns the new pin id.
        """
        cur = self._execute(
            """
            INSERT INTO plex_added_at_pins
                (imported_path, original_added_at, rating_key, request_id, status)
            VALUES (%s, %s, %s, %s, 'pending')
            RETURNING id
            """,
            (imported_path, int(original_added_at), rating_key, request_id),
        )
        row = cur.fetchone()
        assert row is not None, "INSERT RETURNING should always return a row"
        return row["id"]

    def get_pending_plex_added_at_pins(
        self,
        *,
        captured_before: datetime,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return pending pins captured before ``captured_before``, oldest first.

        The ``captured_before`` cutoff is the reconciler's settle window — a pin
        is only acted on once enough time has passed for the Plex partial scan
        triggered alongside its capture to have completed.
        """
        cur = self._execute(
            """
            SELECT id, request_id, imported_path, original_added_at,
                   rating_key, status, captured_at, reconciled_at
            FROM plex_added_at_pins
            WHERE status = 'pending' AND captured_at < %s
            ORDER BY captured_at ASC, id ASC
            LIMIT %s
            """,
            (captured_before, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]

    def mark_plex_added_at_pin(
        self,
        pin_id: int,
        *,
        status: PlexTerminalPinStatus,
        reconciled_at: datetime,
    ) -> None:
        """Mark a pin terminal: ``status`` is 'done' (pinned/already-correct)
        or 'skipped' (album no longer locatable in Plex)."""
        self._execute(
            """
            UPDATE plex_added_at_pins
            SET status = %s, reconciled_at = %s
            WHERE id = %s
            """,
            (status, reconciled_at, int(pin_id)),
        )

    def prune_terminal_plex_added_at_pins(
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
            DELETE FROM plex_added_at_pins
            WHERE status = ANY(%s)
              AND reconciled_at < %s
            """,
            (list(PLEX_TERMINAL_PIN_STATUSES), older_than),
        )
        return cur.rowcount
