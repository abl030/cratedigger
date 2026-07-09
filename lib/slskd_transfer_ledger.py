"""slskd transfer write-ahead ledger maintenance (issue #571, T3).

This module owns ONLY the bounded-retention prune -- it does not cancel
slskd transfers, delete disk files, or infer ownership. Those are the
convergence/reaper/purge flips (three separate follow-up PRs); this PR
is the enabler only. Pruning shrinks cratedigger's own bookkeeping table
and touches nothing outside PostgreSQL, so it is safe to run every cycle
on its own, ahead of the flips that will actually consult the ledger.

See migration 045 and ``lib/pipeline_db/transfer_ledger.py`` for the
schema and ``prune_transfer_ledger``'s policy (old AND request no longer
active).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


# Module-level constant, no config knob (single-operator doctrine --
# .claude/rules/scope.md). 90 days comfortably outlives any retry window
# the future reaper/convergence flips are expected to need, while still
# keeping the table from growing unbounded.
TRANSFER_LEDGER_PRUNE_RETENTION_DAYS: int = 90


def prune_transfer_ledger_cycle(ctx: "CratediggerContext") -> int:
    """Phase 0d: hard-delete transfer-ledger rows past retention whose
    request is no longer active.

    Best-effort -- never raises for a DB failure; logs and returns 0 so
    a prune hiccup never blocks the cycle (matching every other Phase 0
    sweep's contract). Returns the number of rows removed (0 on failure
    or when nothing qualified).
    """
    db = ctx.pipeline_db_source._get_db()
    cutoff = (
        datetime.now(timezone.utc)
        - timedelta(days=TRANSFER_LEDGER_PRUNE_RETENTION_DAYS)
    )
    try:
        removed = db.prune_transfer_ledger(older_than=cutoff)
    except Exception:
        logger.warning(
            "TRANSFER-LEDGER: prune failed", exc_info=True)
        return 0
    if removed:
        logger.info("TRANSFER-LEDGER: pruned %d row(s) past retention", removed)
    return removed
