"""Cycle-summary log line formatting."""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.context import CratediggerContext


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
