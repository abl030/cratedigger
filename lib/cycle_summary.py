"""Cycle-summary log line formatting.

R13/R15 from issue #198: emit one grep-friendly key=value line at end of cycle
attributing wall time to browse / match / cache-load / search phases plus
fan-out wave counters. Pure function — testable without a real cycle.
"""
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
        f"cache_load_s={ctx.cache_load_s:.1f} "
        f"peers_browsed={ctx.peers_browsed} "
        f"peers_browsed_lazy={ctx.peers_browsed_lazy} "
        f"fanout_waves={ctx.fanout_waves} "
        f"cycle_deadline_skipped={ctx.cycle_deadline_skipped} "
        f"cycle_total_s={elapsed_s:.1f}"
    )
