"""Pure decisions the pipeline DB makes inside its own transactions.

Every function here is the ONE spelling of a rule that used to be written
twice: once by production (inside a SQL statement, or in the Python tail of
one) and once by hand in ``tests/fakes/pipeline_db.py``. Extracting the
DECISION does not move the transaction — the ``FOR UPDATE`` re-read, the
compare-and-set, and the aggregate all stay exactly where they were. The
production adapter calls a function here between its reads and its writes;
the fake calls the same function from inside its own snapshot machinery.

Nothing in this module touches a connection, a clock, or a filesystem, so
every rule is directly table-testable and directly property-testable.
"""

from __future__ import annotations

from dataclasses import dataclass

from lib.pipeline_db._shared import (
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    CURSOR_UPDATE_ADVANCED,
    CURSOR_UPDATE_STALE,
    CURSOR_UPDATE_WRAPPED,
    SEARCH_LOG_STAGE_ACCEPTED,
    SEARCH_LOG_STAGE_STALE_COMPLETION,
    SaturationSummary,
)

# ---------------------------------------------------------------------------
# Retry pacing
# ---------------------------------------------------------------------------


def _search_backoff_max_exponent() -> int:
    """Smallest doubling count that already reaches ``BACKOFF_MAX_MINUTES``.

    Past this exponent the ``min()`` cap is unconditional, so clamping the
    exponent cannot change any returned value. Python does not need the
    clamp (its ints are arbitrary precision) — PostgreSQL does: ``POWER``
    resolves to ``double precision``, and ``LEAST(30 * POWER(2, 1024), 240)``
    raises ``value out of range: overflow`` rather than capping (measured
    against the live database, 2026-08-31). The worst live attempt counter
    the same day was 407, and the 4-hour cap admits at most six attempts a
    day per unfound request — so the unclamped expression was months from
    erroring, not structurally unreachable.

    ``lib/import_queue.py::_import_preview_requeue_max_exponent`` derives
    its own cap the same way for the preview-requeue family. The two are
    deliberately not shared: they carry different constants and units, and
    ``lib.import_queue`` cannot import from ``lib.pipeline_db`` (every
    pipeline-DB mixin already imports ``lib.import_queue``, so the edge
    would close an import cycle).
    """
    if BACKOFF_MAX_MINUTES <= BACKOFF_BASE_MINUTES:
        return 0
    ceiling_ratio = (
        BACKOFF_MAX_MINUTES + BACKOFF_BASE_MINUTES - 1
    ) // BACKOFF_BASE_MINUTES
    return (ceiling_ratio - 1).bit_length()


#: Exponent ceiling handed to the SQL retry-pacing writers as a bound
#: parameter. See ``_search_backoff_max_exponent``.
SEARCH_BACKOFF_MAX_EXPONENT: int = _search_backoff_max_exponent()


def search_backoff_minutes(prior_attempts: int) -> int:
    """Minutes to wait before the next attempt, given the prior count.

    ``prior_attempts`` is always the counter's value BEFORE this attempt is
    added: the SQL writers read ``COALESCE(<counter>, 0)`` in the same
    ``UPDATE`` that increments it (PostgreSQL evaluates ``SET`` expressions
    against the old row), and the Python writers pass either their own
    ``prior_attempts`` local or ``new_count - 1`` after incrementing. So a
    request that has never been attempted waits ``BACKOFF_BASE_MINUTES``.

    Negative input is rejected rather than silently producing a fractional
    interval. No production caller can reach it: every one derives the
    argument from a non-negative counter it has just read or incremented.
    """
    if prior_attempts < 0:
        raise ValueError(
            f"prior_attempts must be non-negative, got {prior_attempts}"
        )
    exponent = min(prior_attempts, SEARCH_BACKOFF_MAX_EXPONENT)
    return min(BACKOFF_BASE_MINUTES * (2 ** exponent), BACKOFF_MAX_MINUTES)


# ---------------------------------------------------------------------------
# Consumed-search-attempt cursor
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CursorAdvanceDecision:
    """What one consumed search attempt does to the request's plan cursor.

    ``@dataclass`` rather than ``msgspec.Struct``: it is built from typed
    Python and consumed by the same transaction that built it; it never
    crosses JSON (``.claude/rules/code-quality.md`` § wire-boundary types).
    """

    cursor_update_status: str
    execution_stage: str
    stale_reason: str | None
    new_next_ordinal: int
    new_cycle_count: int

    @property
    def is_stale(self) -> bool:
        return self.cursor_update_status == CURSOR_UPDATE_STALE


def cursor_advance_decision(
    *,
    current_status: str,
    active_plan_id: int | None,
    next_ordinal: int,
    cycle_count: int,
    attempt_plan_id: int,
    attempt_plan_ordinal: int,
    attempt_cycle_count_snapshot: int,
    attempt_plan_item_count: int,
) -> CursorAdvanceDecision:
    """Decide stale / advanced / wrapped for one consumed plan attempt.

    The first four arguments are the request's CURRENT cursor state, re-read
    under the row lock; the rest describe the attempt that just finished.
    An attempt is stale when the request was superseded (``replaced``) or
    when any of plan, ordinal, or cycle moved under it — a stale attempt is
    still logged, but leaves the cursor exactly where it found it.

    A live attempt advances the ordinal by one, except on the last item of
    the plan, where it wraps to ordinal 0 and opens the next cycle. A plan
    the caller reports as having zero items is pathological (the generator's
    CHECK and the service contract prevent it); it advances rather than
    dividing by a plan length of zero.
    """
    is_stale = (
        current_status == "replaced"
        or active_plan_id != attempt_plan_id
        or next_ordinal != attempt_plan_ordinal
        or cycle_count != attempt_cycle_count_snapshot
    )
    if is_stale:
        return CursorAdvanceDecision(
            cursor_update_status=CURSOR_UPDATE_STALE,
            execution_stage=SEARCH_LOG_STAGE_STALE_COMPLETION,
            stale_reason=(
                "request_replaced"
                if current_status == "replaced"
                else "regenerated"
            ),
            new_next_ordinal=next_ordinal,
            new_cycle_count=cycle_count,
        )
    item_count = max(int(attempt_plan_item_count), 0)
    if item_count and attempt_plan_ordinal >= item_count - 1:
        return CursorAdvanceDecision(
            cursor_update_status=CURSOR_UPDATE_WRAPPED,
            execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
            stale_reason=None,
            new_next_ordinal=0,
            new_cycle_count=cycle_count + 1,
        )
    return CursorAdvanceDecision(
        cursor_update_status=CURSOR_UPDATE_ADVANCED,
        execution_stage=SEARCH_LOG_STAGE_ACCEPTED,
        stale_reason=None,
        new_next_ordinal=next_ordinal + 1,
        new_cycle_count=cycle_count,
    )


# ---------------------------------------------------------------------------
# Plan-readiness buckets
# ---------------------------------------------------------------------------

PLAN_READINESS_SEARCHABLE = "wanted_searchable"
PLAN_READINESS_LEGACY = "wanted_legacy"
PLAN_READINESS_FAILED_DETERMINISTIC = "wanted_failed_deterministic"
PLAN_READINESS_FAILED_TRANSIENT = "wanted_failed_transient"
PLAN_READINESS_NO_PLAN = "wanted_no_plan"

#: Every bucket a wanted request can land in, in precedence order. The
#: readiness payload carries one count per name plus ``wanted_total``.
PLAN_READINESS_BUCKETS: tuple[str, ...] = (
    PLAN_READINESS_SEARCHABLE,
    PLAN_READINESS_LEGACY,
    PLAN_READINESS_FAILED_DETERMINISTIC,
    PLAN_READINESS_FAILED_TRANSIENT,
    PLAN_READINESS_NO_PLAN,
)


def classify_plan_readiness_bucket(
    *,
    active_plan_generator_id: str | None,
    current_generator_id: str,
    has_failed_deterministic: bool,
    has_failed_transient: bool,
) -> str:
    """Bucket one wanted request for the plan-readiness dashboard.

    ``active_plan_generator_id`` is the generator id resolved THROUGH the
    request's ``active_plan_id`` — ``None`` when the request has no active
    plan, or when the pointer resolves to no plan row. Mirroring the SQL's
    ``active_plan.generator_id IS NOT NULL`` guard exactly is what keeps an
    unresolved pointer out of the ``wanted_legacy`` bucket; ``!=`` alone
    would call a missing generator "old generator". ``search_plans
    .generator_id`` is ``NOT NULL`` (migration 014), so the unresolved case
    is reachable only through the join, never through a null column.

    The two failed-plan facts are only consulted once the request has no
    resolved active generator, which is why the SQL's ``EXISTS`` arms sit
    after both active-plan arms in one ``CASE``.
    """
    if active_plan_generator_id is not None:
        if active_plan_generator_id == current_generator_id:
            return PLAN_READINESS_SEARCHABLE
        return PLAN_READINESS_LEGACY
    if has_failed_deterministic:
        return PLAN_READINESS_FAILED_DETERMINISTIC
    if has_failed_transient:
        return PLAN_READINESS_FAILED_TRANSIENT
    return PLAN_READINESS_NO_PLAN


# ---------------------------------------------------------------------------
# Saturation summary
# ---------------------------------------------------------------------------


def saturation_summary_from_counts(
    *,
    total_searches: int,
    saturated_searches: int,
    total_pre_filter_skips: int,
    window_days: int,
) -> SaturationSummary:
    """Assemble the saturation payload from one window's counts.

    The window cut itself stays with each adapter — production asks
    PostgreSQL (``created_at > NOW() - make_interval(...)``), the fake walks
    its own rows against a rewindable Python clock — because only the rate
    arithmetic and the explicit zero-total fallback are a shared decision.
    ``saturation_rate`` is ``0.0`` rather than NaN on an empty window: the
    payload is serialised to JSON downstream.
    """
    total = int(total_searches)
    saturated = int(saturated_searches)
    return SaturationSummary(
        total_searches=total,
        saturated_searches=saturated,
        saturation_rate=(saturated / total) if total > 0 else 0.0,
        total_pre_filter_skips=int(total_pre_filter_skips),
        window_days=int(window_days),
    )
