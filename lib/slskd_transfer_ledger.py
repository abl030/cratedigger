"""slskd transfer write-ahead ledger maintenance (issue #571, T3).

This module owns ONLY the bounded-retention prune -- it does not cancel
slskd transfers, delete disk files, or infer ownership. Separate
convergence, reaper, and terminal-purge paths consult the ledger. Pruning
shrinks Cratedigger's own bookkeeping table and touches nothing outside
PostgreSQL, so it is safe to run every cycle.

See migrations 045 and 051 and ``lib/pipeline_db/transfer_ledger.py`` for the
schema and ``prune_transfer_ledger``'s policy: old pending intent is always
pruned, while accepted evidence retains active-request protection.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


# Module-level constant, no config knob (single-operator doctrine --
# .claude/rules/scope.md). 90 days comfortably outlives any diagnosis window
# for pending intent and any retry window accepted ownership evidence may need,
# while still keeping old unconfirmed intents and inactive evidence bounded.
TRANSFER_LEDGER_PRUNE_RETENTION_DAYS: int = 90


def prune_transfer_ledger_cycle(ctx: CratediggerContext) -> int:
    """Phase 0d: hard-delete transfer-ledger rows past retention.

    Pending intent is bounded regardless of request status. Accepted evidence
    remains protected while its request is wanted or downloading.

    DB failures deliberately propagate to ``lib/convergence.py``: the registry
    owns cycle-preserving failure isolation, so this step and any future step
    retain the same behavior without local exception wrappers. Returns the
    number of rows removed (0 when nothing qualified).
    """
    db = ctx.pipeline_db_source._get_db()
    cutoff = (
        datetime.now(UTC)
        - timedelta(days=TRANSFER_LEDGER_PRUNE_RETENTION_DAYS)
    )
    removed = db.prune_transfer_ledger(older_than=cutoff)
    if removed:
        logger.info("TRANSFER-LEDGER: pruned %d row(s) past retention", removed)
    return removed
