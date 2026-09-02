"""The per-cycle counters, declared once (issue #1348).

One cratedigger cycle accumulates sixteen numbers. Two consumers used to
spell all sixteen names out by hand and now derive them from the
declaration below: the operator-facing summary line and the
``cycle_metrics`` INSERT. A third, ``lib.enqueue.FindDownloadMetrics``,
still hand-declares the eight it carries back from a worker; it is a
projection by name, not a derivation.

The declaration order IS the summary line's token order and the
``cycle_metrics`` INSERT's column order. It also matches the column
order migration 011 created, which is not load-bearing (the INSERT names
its columns) but is worth keeping so the row reads like the log line.

**Adding a counter is not one edit.** It is this declaration, a
``migrations/NNN`` ``ALTER TABLE cycle_metrics ADD COLUMN`` (without it
the derived INSERT raises ``UndefinedColumn`` on the first cycle after
deploy), and the literals that exist so the derivation cannot quietly
agree with itself: ``EXPECTED_ORDER`` and the zero-defaults count in
``tests/test_cycle_counters.py``, both full-line pins in
``tests/test_cycle_summary.py``, and ``EXPECTED`` in
``tests/test_pipeline_db_column_contract.py``. What derivation buys is
that every one of those failures is LOUD; before it, the line and the
row could each silently drop a counter.

A cycle owns one instance and each find_download worker gets its own.
Nothing forwards it: unlike ``folder_cache`` or ``browse_coordinator``,
which ``prepare_find_download_context`` deliberately shares by
reference, a shared counters value would be double-counted the moment
the owner merged a worker's ``FindDownloadMetrics`` back into it. That
is a property of the derivations, not of the type -- this object is
exactly as shareable as any other -- so it is asserted at both
derivations rather than assumed here.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields
from typing import get_type_hints


@dataclass(kw_only=True)
class CycleCounters:
    """Everything one cycle counts, in summary-line and row order.

    Keyword-only on purpose: sixteen same-typed numeric fields make a
    positional constructor a swap waiting to happen, and a swap between
    two counters is invisible to any assertion that only checks shape.
    """

    # Timing accumulators (issue #198 U1 instrumentation). browse and
    # match are wrapped at the call sites in lib/matching.py; search is
    # wrapped around _search_and_queue_parallel in cratedigger.py.
    browse_time_s: float = 0.0
    match_time_s: float = 0.0
    search_time_s: float = 0.0

    # Redis peer-cache outcomes, drained off the cache by
    # lib/peer_cache.py::drain_stats_into_context.
    cache_pos_hits: int = 0
    cache_neg_hits: int = 0
    cache_misses: int = 0
    cache_errors: int = 0
    cache_fuse_tripped: int = 0
    cache_write_errors: int = 0

    # peers_browsed counts actual cold slskd directory submissions from
    # the primary fan-out path. Redis hits, Redis negative skips, and
    # duplicate callers that join existing in-flight browses do not
    # increment it. peers_browsed_lazy tracks residual cold submissions
    # from the fallback path in lib/matching.py.
    peers_browsed: int = 0
    peers_browsed_lazy: int = 0
    fanout_waves: int = 0

    # Search-watchdog firings (issue #212), one per ``SearchResult`` with
    # ``watchdog_fired=True``. Replaces the ``cycle_deadline_skipped``
    # counter that fed the rolled-back ``cycle_max_runtime_s`` cycle-entry
    # gate. Healthy steady-state is 0-1 per cycle; >3 sustained warrants
    # investigation.
    cycle_searches_watchdog_killed: int = 0

    # find_download pipeline throughput (issue #217).
    find_download_queued: int = 0
    find_download_completed: int = 0
    find_download_drain_time_s: float = 0.0


#: Every counter name, in declaration order. The summary line's tokens
#: and the ``cycle_metrics`` INSERT's counter columns are both this list.
COUNTER_NAMES: tuple[str, ...] = tuple(f.name for f in fields(CycleCounters))


def _float_counter_names(hints: Mapping[str, type[object]]) -> frozenset[str]:
    """The counters declared ``float``, refusing any other declared type.

    Read from the declared type, never from the current value: a test
    that assigns ``browse_time_s = 12`` must still render ``12.0``, and
    the ``cycle_metrics`` column type follows the declaration too.
    Failing closed on a third type is what stops a new counter from
    reaching the operator's line in a format nobody chose.
    """
    unsupported = sorted(
        name for name in COUNTER_NAMES if hints[name] not in (int, float))
    if unsupported:
        raise TypeError(
            "cycle counters must be declared int or float; "
            f"cannot render or persist: {', '.join(unsupported)}")
    return frozenset(name for name in COUNTER_NAMES if hints[name] is float)


#: Counters rendered with one decimal place and stored as DOUBLE
#: PRECISION. Everything else is an integer count.
FLOAT_COUNTER_NAMES: frozenset[str] = _float_counter_names(
    get_type_hints(CycleCounters))


def counter_values(counters: CycleCounters) -> list[float]:
    """Every counter's value, in ``COUNTER_NAMES`` order."""
    return [getattr(counters, name) for name in COUNTER_NAMES]
