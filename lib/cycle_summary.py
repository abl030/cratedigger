"""Cycle close-out reporting: the summary line and its persisted twins.

The three registered end-of-cycle steps below (summary line, cycle-metrics
row, browsed-peer roster) read ``ctx.counters`` and the wall-clock anchors
``cycle_started_at``/``cycle_start`` that ``cratedigger.run_cycle`` sets as
the cycle body starts. The line and the row are both derived from
``lib.cycle_counters``, so they cannot disagree about which counters exist.
Failures deliberately propagate to ``lib/convergence.py``: the registry owns
cycle-preserving failure isolation.
"""
from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from lib.cycle_counters import (
    COUNTER_NAMES,
    FLOAT_COUNTER_NAMES,
    CycleCounters,
    counter_values,
)

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")

#: The prefix log scrapers match on. ``scripts/verify_cratedigger_cycle.sh``
#: and the runbook in ``docs/nixos-module.md`` both look for this substring
#: and nothing else, so the tokens after it may grow, but this may not move.
CYCLE_COMPLETE_PREFIX = "Cratedigger cycle complete in"


def format_cycle_summary(counters: CycleCounters, elapsed_s: float) -> str:
    """Render the per-cycle summary line.

    Prefix preserves the existing human-readable string so log scrapers
    parsing 'Cratedigger cycle complete in Ns' continue to match. Every
    counter follows as a space-separated ``key=value`` pair, in
    declaration order, floats to one decimal place. Nothing here lists
    the counters: declaring one in ``lib.cycle_counters`` logs it.
    """
    tokens = [
        f"{name}={value:.1f}" if name in FLOAT_COUNTER_NAMES
        else f"{name}={value}"
        for name, value in zip(
            COUNTER_NAMES, counter_values(counters), strict=True)
    ]
    return (
        f"{CYCLE_COMPLETE_PREFIX} {elapsed_s:.1f}s "
        + " ".join(tokens)
        + f" cycle_total_s={elapsed_s:.1f}"
    )


def log_cycle_summary(ctx: CratediggerContext) -> str:
    """Registered end-of-cycle step: emit the canonical summary line."""
    line = format_cycle_summary(ctx.counters, time.time() - ctx.cycle_start)
    logger.info(line)
    return line


def record_cycle_metrics_cycle(ctx: CratediggerContext) -> None:
    """Registered end-of-cycle step: persist the cycle-metrics row."""
    db = ctx.pipeline_db_source._get_db()
    db.record_cycle_metrics(
        counters=ctx.counters,
        started_at=ctx.cycle_started_at,
        completed_at=datetime.now(UTC),
        cycle_total_s=time.time() - ctx.cycle_start,
    )


def record_peer_observations_cycle(ctx: CratediggerContext) -> int:
    """Registered end-of-cycle step: flush the cycle's browsed-peer roster."""
    observations = ctx.peer_observations
    if not observations:
        return 0
    db = ctx.pipeline_db_source._get_db()
    new_observations = db.record_peer_observations(
        observations, observed_at=datetime.now(UTC))
    logger.info(
        "Peer observations persisted: observed=%d new=%d",
        len(observations), new_observations)
    return new_observations
