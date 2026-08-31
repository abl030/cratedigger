"""Cycle close-out reporting: the summary line and its persisted twins.

The three registered end-of-cycle steps below (summary line, cycle-metrics
row, browsed-peer roster) read the per-cycle accumulators off the context and
the wall-clock anchors ``cycle_started_at``/``cycle_start`` that ``main()``
sets as the cycle body starts. Failures deliberately propagate to
``lib/convergence.py``: the registry owns cycle-preserving failure isolation.
"""
from __future__ import annotations

import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


def format_cycle_summary(ctx: CratediggerContext, elapsed_s: float) -> str:
    """Render the per-cycle summary line.

    Prefix preserves the existing human-readable string so log scrapers
    parsing 'Cratedigger cycle complete in Ns' continue to match. New keys
    are appended as space-separated `key=value` pairs.
    """
    return (
        f"Cratedigger cycle complete in {elapsed_s:.1f}s "
        f"browse_time_s={ctx.browse_time_s:.1f} "
        f"match_time_s={ctx.match_time_s:.1f} "
        f"search_time_s={ctx.search_time_s:.1f} "
        f"cache_pos_hits={ctx.cache_pos_hits} "
        f"cache_neg_hits={ctx.cache_neg_hits} "
        f"cache_misses={ctx.cache_misses} "
        f"cache_errors={ctx.cache_errors} "
        f"cache_fuse_tripped={ctx.cache_fuse_tripped} "
        f"cache_write_errors={ctx.cache_write_errors} "
        f"peers_browsed={ctx.peers_browsed} "
        f"peers_browsed_lazy={ctx.peers_browsed_lazy} "
        f"fanout_waves={ctx.fanout_waves} "
        f"cycle_searches_watchdog_killed={ctx.cycle_searches_watchdog_killed} "
        f"find_download_queued={ctx.find_download_queued} "
        f"find_download_completed={ctx.find_download_completed} "
        f"find_download_drain_time_s={ctx.find_download_drain_time_s:.1f} "
        f"cycle_total_s={elapsed_s:.1f}"
    )


def log_cycle_summary(ctx: CratediggerContext) -> str:
    """Registered end-of-cycle step: emit the canonical summary line."""
    line = format_cycle_summary(ctx, time.time() - ctx.cycle_start)
    logger.info(line)
    return line


def record_cycle_metrics_cycle(ctx: CratediggerContext) -> None:
    """Registered end-of-cycle step: persist the cycle-metrics row."""
    db = ctx.pipeline_db_source._get_db()
    db.record_cycle_metrics(
        started_at=ctx.cycle_started_at,
        completed_at=datetime.now(UTC),
        cycle_total_s=time.time() - ctx.cycle_start,
        browse_time_s=ctx.browse_time_s,
        match_time_s=ctx.match_time_s,
        search_time_s=ctx.search_time_s,
        cache_pos_hits=ctx.cache_pos_hits,
        cache_neg_hits=ctx.cache_neg_hits,
        cache_misses=ctx.cache_misses,
        cache_errors=ctx.cache_errors,
        cache_fuse_tripped=ctx.cache_fuse_tripped,
        cache_write_errors=ctx.cache_write_errors,
        peers_browsed=ctx.peers_browsed,
        peers_browsed_lazy=ctx.peers_browsed_lazy,
        fanout_waves=ctx.fanout_waves,
        cycle_searches_watchdog_killed=ctx.cycle_searches_watchdog_killed,
        find_download_queued=ctx.find_download_queued,
        find_download_completed=ctx.find_download_completed,
        find_download_drain_time_s=ctx.find_download_drain_time_s,
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
