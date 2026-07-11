"""Bounded retention for terminal Plex/Jellyfin convergence pins."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")

# Pin rows are convergence bookkeeping, not operator audit history. Match the
# transfer-ledger's established fixed 90-day retention convention: long enough
# for operational diagnosis, bounded forever, and no single-operator config
# knob for policy that does not vary by deployment.
PIN_RETENTION_DAYS = 90


def prune_terminal_pin_rows_cycle(
    ctx: "CratediggerContext",
    *,
    now: datetime | None = None,
) -> int:
    """Phase 0: prune strictly old terminal rows from both pin stores.

    DB failures deliberately propagate to ``lib/convergence.py``: the registry
    owns cycle-preserving failure isolation, so this step and any future step
    retain the same behavior without local exception wrappers.
    """
    reference = now or datetime.now(timezone.utc)
    cutoff = reference - timedelta(days=PIN_RETENTION_DAYS)
    db = ctx.pipeline_db_source._get_db()
    plex_removed = db.prune_terminal_plex_added_at_pins(older_than=cutoff)
    jellyfin_removed = db.prune_terminal_jellyfin_date_created_pins(
        older_than=cutoff)
    removed = plex_removed + jellyfin_removed
    if removed:
        logger.info(
            "PIN-RETENTION: pruned %d Plex and %d Jellyfin terminal row(s)",
            plex_removed,
            jellyfin_removed,
        )
    return removed
