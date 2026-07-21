"""Search-plan lifecycle, cursor, search_log, attempts, saturation."""
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Sequence, TypedDict
import psycopg2
import psycopg2.extras


if TYPE_CHECKING:
    from lib.quality import CandidateScore

from lib.import_queue import (
    IMPORT_JOB_YOUTUBE,
)

from lib.search_classification import (
    SearchSummary as _SearchSummary,
    classify_failure_class as _classify_failure_class,
)
from lib.search_scheduler import (
    NEW_REQUEST_PRIORITY_HOURS,
    search_cohort_slots,
)

from lib.pipeline_db._shared import (
    ActiveSearchPlan,
    BACKOFF_BASE_MINUTES,
    BACKOFF_MAX_MINUTES,
    CURSOR_UPDATE_ADVANCED,
    CURSOR_UPDATE_STALE,
    CURSOR_UPDATE_UNCHANGED,
    CURSOR_UPDATE_WRAPPED,
    ConsumedAttemptInput,
    ConsumedAttemptResult,
    DryRunPlanClassification,
    NonConsumingAttemptInput,
    PLAN_STATUS_ACTIVE,
    PLAN_STATUS_FAILED_DETERMINISTIC,
    PLAN_STATUS_FAILED_TRANSIENT,
    PLAN_STATUS_SUPERSEDED,
    ReplacedRequestMutationError,
    SEARCH_LOG_STAGE_ACCEPTED,
    SEARCH_LOG_STAGE_PRE_ATTEMPT,
    SEARCH_LOG_STAGE_STALE_COMPLETION,
    SaturationSummary,
    SearchLogHistoryPage,
    SearchPlanInspection,
    SearchPlanItemInput,
    SearchPlanItemProvenance,
    SearchPlanItemRow,
    SearchPlanMetadataSnapshot,
    SearchPlanProvenance,
    SearchPlanRow,
    SearchPlanStats,
    WantedReconciliationCandidate,
    _build_stats_bucket,
    _item_provenance_from_jsonb,
    _json_param,
    _metadata_snapshot_from_jsonb,
    _plan_provenance_from_jsonb,
    pg_execute_values,
)

from lib.pipeline_db._core import _PipelineDBBase


class _ReadinessBucketRow(TypedDict):
    """One ``get_search_plan_readiness`` aggregate row: bucket counts for
    the wanted cohort under one generator id. The CTE always returns
    exactly one row (no ``GROUP BY``); the empty-fallback below only
    matters if the query somehow returns zero rows."""

    wanted_total: int
    wanted_searchable: int
    wanted_legacy: int
    wanted_failed_deterministic: int
    wanted_failed_transient: int
    wanted_no_plan: int


_EMPTY_READINESS_BUCKET_ROW: _ReadinessBucketRow = {
    "wanted_total": 0,
    "wanted_searchable": 0,
    "wanted_legacy": 0,
    "wanted_failed_deterministic": 0,
    "wanted_failed_transient": 0,
    "wanted_no_plan": 0,
}


class _SearchPlanMixin(_PipelineDBBase):
    """Search-plan lifecycle, cursor, search_log, attempts, saturation."""


    # -- Search log -----------------------------------------------------------

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error",
                   candidates: "list[CandidateScore] | None" = None,
                   variant: str | None = None,
                   final_state: str | None = None,
                   browse_time_s: float = 0.0,
                   match_time_s: float = 0.0,
                   peers_browsed: int = 0,
                   peers_browsed_lazy: int = 0,
                   fanout_waves: int = 0,
                   pre_filter_skip_count: int = 0,
                   rejection_reason: str | None = None,
                   result_count_uncapped: int | None = None,
                   query_token_count: int | None = None,
                   query_distinct_token_count: int | None = None,
                   expected_track_count: int | None = None,
                   matcher_score_top1: float | None = None,
                   query_template: str | None = None) -> None:
        """Record one search attempt for an album request.

        ``candidates`` is the top-N forensic ``CandidateScore`` list (already
        truncated by the caller). It is encoded via ``msgspec.json.encode``
        and written to ``search_log.candidates`` JSONB. ``None`` writes SQL
        NULL — error / submission-failure rows have no scoring data to
        report. See ``.claude/rules/code-quality.md`` § Wire-boundary types
        for the symmetric encode/decode contract.

        ``pre_filter_skip_count`` (U2 of search-plan-entropy) is the
        aggregate count of dirs the matcher's asymmetric pre-filter
        rejected before browse. NOT NULL on the column; default 0 keeps
        pre-attempt / error rows uniformly populated.

        ``rejection_reason`` (R22), ``result_count_uncapped`` (R23),
        ``query_token_count`` / ``query_distinct_token_count`` (R24),
        ``expected_track_count`` (R25), ``matcher_score_top1`` (R26),
        and ``query_template`` (R27) are the U11 forensics columns
        added in migration 027. All nullable; default ``None`` writes
        SQL NULL so historical-style callers (and unit tests that only
        exercise the candidate JSONB) stay backwards-compatible while
        production callers populate every field.
        """
        candidates_json: str | None = None
        if candidates is not None:
            import msgspec  # local import keeps top-of-module deps narrow
            candidates_json = msgspec.json.encode(candidates).decode()
        self._execute("""
            INSERT INTO search_log (
                request_id, query, result_count, elapsed_s, outcome,
                candidates, variant, final_state, browse_time_s, match_time_s,
                peers_browsed, peers_browsed_lazy, fanout_waves,
                pre_filter_skip_count,
                rejection_reason, result_count_uncapped,
                query_token_count, query_distinct_token_count,
                expected_track_count, matcher_score_top1, query_template
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s)
        """, (request_id, query, result_count, elapsed_s, outcome,
              candidates_json, variant, final_state, browse_time_s, match_time_s,
              peers_browsed, peers_browsed_lazy, fanout_waves,
              pre_filter_skip_count,
              rejection_reason, result_count_uncapped,
              query_token_count, query_distinct_token_count,
              expected_track_count, matcher_score_top1, query_template))
        self.conn.commit()


    def get_search_history(self, request_id: int) -> list[dict[str, object]]:
        """Return all search_log rows for a single request_id, newest first."""
        cur = self._execute("""
            SELECT * FROM search_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]


    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, object]]:
        """Projection-only search_log rows needed for search-plan stats.

        Intentionally excludes candidates JSONB so inspection endpoints do
        not deserialize every candidate blob just to compute aggregate stats.
        """
        cur = self._execute("""
            SELECT id, request_id, query, result_count, elapsed_s, outcome,
                   variant, final_state, browse_time_s, match_time_s,
                   peers_browsed, peers_browsed_lazy, fanout_waves,
                   plan_id, plan_item_id, plan_ordinal, plan_strategy,
                   plan_canonical_query_key, plan_repeat_group,
                   plan_generator_id, execution_stage, attempt_consumed,
                   cursor_update_status, stale_reason, plan_cycle_snapshot,
                   created_at
            FROM search_log
            WHERE request_id = %s
            ORDER BY id DESC
        """, (request_id,))
        return [dict(r) for r in cur.fetchall()]


    def get_saturation_summary(
        self, request_id: int, *, window_days: int = 14,
    ) -> "SaturationSummary":
        """U7: saturation rate + pre-filter skip total for one request.

        Aggregates ``search_log`` rows for ``request_id`` whose
        ``created_at`` falls inside the last ``window_days`` days.
        Returns a :class:`SaturationSummary` with:

        * ``total_searches`` — count of rows in window
        * ``saturated_searches`` — rows whose ``final_state`` matches
          ``%LimitReached%`` (slskd hit its response/file ceiling)
        * ``saturation_rate`` — ratio in ``[0.0, 1.0]``; ``0.0`` when
          ``total_searches == 0`` (explicit, NOT NaN — the value is
          serialised to JSON downstream)
        * ``total_pre_filter_skips`` — sum of the U2
          ``pre_filter_skip_count`` column over the same window
        * ``window_days`` — echoed back so callers don't need to
          remember which window they asked for

        ``window_days`` is a non-negative int. The route handler and
        CLI both bound it before calling (negative or huge values are
        operator errors); the DB layer trusts the caller. ``0`` reduces
        the SQL to an empty window and returns all zeros — that's a
        defensible read (operator asked for the past zero days).

        Returns an empty summary (all zeros, ``saturation_rate=0.0``)
        when the request has no logged searches in window. This method
        does NOT consult ``album_requests`` — the caller decides
        whether to translate "no rows" into a 404 by looking up the
        request row separately. See the service-layer
        ``SearchPlanService.saturation_for_request`` for that mapping.
        """
        cur = self._execute("""
            SELECT
              COUNT(*) AS total_searches,
              COUNT(*) FILTER (WHERE final_state LIKE %s)
                AS saturated_searches,
              COALESCE(SUM(pre_filter_skip_count), 0)
                AS total_pre_filter_skips
            FROM search_log
            WHERE request_id = %s
              AND created_at > NOW() - make_interval(days => %s)
        """, ("%LimitReached%", request_id, int(window_days)))
        row = cur.fetchone()
        if row is None:
            # Defensive — aggregate queries always return one row, but
            # keep the type-checker happy and the helper total.
            return SaturationSummary(
                total_searches=0,
                saturated_searches=0,
                saturation_rate=0.0,
                total_pre_filter_skips=0,
                window_days=int(window_days),
            )
        total = int(row["total_searches"] or 0)
        saturated = int(row["saturated_searches"] or 0)
        rate = (saturated / total) if total > 0 else 0.0
        return SaturationSummary(
            total_searches=total,
            saturated_searches=saturated,
            saturation_rate=rate,
            total_pre_filter_skips=int(row["total_pre_filter_skips"] or 0),
            window_days=int(window_days),
        )


    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, object]]]:
        """Return count + bounded head sample of legacy search_log rows."""
        count_cur = self._execute("""
            SELECT COUNT(*) AS c
            FROM search_log
            WHERE request_id = %s AND plan_id IS NULL
        """, (request_id,))
        count_row = count_cur.fetchone()
        count = int(count_row["c"]) if count_row is not None else 0
        head_cur = self._execute("""
            SELECT id, request_id, query, result_count, elapsed_s, outcome,
                   variant, final_state, created_at
            FROM search_log
            WHERE request_id = %s AND plan_id IS NULL
            ORDER BY id DESC
            LIMIT %s
        """, (request_id, int(limit)))
        return count, [dict(r) for r in head_cur.fetchall()]


    def get_search_history_page(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> "SearchLogHistoryPage":
        """Cursor-paginated ``search_log`` rows for one request.

        Returns at most ``limit`` rows for ``request_id`` ordered by
        ``id DESC`` (newest first). ``before_id`` excludes rows whose id
        is greater-than ``before_id`` — pass the previous page's
        ``next_before_id`` to read the next page.

        ``next_before_id`` is the id of the *next-older row past the
        page boundary* — the +1 row trimmed by the SQL ``LIMIT %s + 1``
        — when more rows remain, or ``None`` when the page exhausted
        the history. The WHERE clause uses ``id <= %s`` (inclusive) so
        ``next_before_id`` round-trips as the smallest id we still want
        to return on the next page. No row is skipped at page
        boundaries.

        ``limit`` is unconditionally cast to ``int`` and not range-checked
        here; callers (the route handler / CLI subcommand) own the
        ``[1, 200]`` clamp so the DB layer stays a thin SQL adapter.
        """
        # +1 row to detect "more remains" without a second COUNT(*) round-trip.
        sql_limit = int(limit) + 1
        # ``<=`` so the trimmed row's id round-trips through the cursor
        # without being dropped on the next page. The strict-less option
        # would skip one row per boundary.
        cur = self._execute("""
            SELECT * FROM search_log
            WHERE request_id = %s
              AND (%s::int IS NULL OR id <= %s)
            ORDER BY id DESC
            LIMIT %s
        """, (request_id, before_id, before_id, sql_limit))
        rows = [dict(r) for r in cur.fetchall()]
        next_before_id: int | None = None
        if len(rows) > int(limit):
            extra = rows.pop()
            extra_id = extra["id"]
            assert isinstance(extra_id, int)
            next_before_id = extra_id
        return SearchLogHistoryPage(
            rows=rows, next_before_id=next_before_id,
        )


    def get_search_plan_readiness(
        self,
        generator_id: str,
    ) -> dict[str, Any]:
        """Aggregate plan-readiness counts for the wanted bucket.

        Bucket precedence (each wanted row falls into exactly one bucket):

          1. ``wanted_searchable`` -- ``status='wanted'`` AND active plan
             whose ``generator_id`` matches the current generator id.
          2. ``wanted_legacy`` -- has an active plan but its ``generator_id``
             differs from the current id (old-generator carryover that
             startup reconciliation will supersede next pass).
          3. ``wanted_failed_deterministic`` -- no active plan AND a
             ``failed_deterministic`` plan exists for the current generator
             id. Sticky; cannot be re-tried by reconciliation.
          4. ``wanted_failed_transient`` -- no active plan AND a
             ``failed_transient`` plan exists for the current generator id.
             Reconciliation will retry next cycle.
          5. ``wanted_no_plan`` -- no active plan AND no current-generator
             plan rows at all. This is the stop-the-deploy signal.

        ``wanted_total`` equals the sum of buckets. The total is read off
        ``album_requests`` directly so any drift between sum and total is
        a bug (drop-the-buckets-on-the-floor classifier mistake) and not
        a missing row.

        Read-only and dashboard-grade: one SQL query. Callers should not
        treat any zero count as proof of post-cutover correctness; pair
        this with ``docs/persisted-search-plans-rollout.md`` SQL spot
        checks (active-plan FK integrity, contiguous ordinals, post-deploy
        ``outcome='exhausted'`` rate).
        """
        cur = self._execute(
            """
            WITH wanted AS (
                SELECT id, active_plan_id
                FROM album_requests
                WHERE status = 'wanted'
            ),
            classified AS (
                SELECT
                    w.id,
                    CASE
                        WHEN w.active_plan_id IS NOT NULL
                             AND active_plan.generator_id = %s
                            THEN 'wanted_searchable'
                        WHEN w.active_plan_id IS NOT NULL
                             AND active_plan.generator_id IS NOT NULL
                             AND active_plan.generator_id <> %s
                            THEN 'wanted_legacy'
                        WHEN EXISTS (
                            SELECT 1 FROM search_plans sp
                            WHERE sp.request_id = w.id
                              AND sp.generator_id = %s
                              AND sp.status = 'failed_deterministic'
                        )
                            THEN 'wanted_failed_deterministic'
                        WHEN EXISTS (
                            SELECT 1 FROM search_plans sp
                            WHERE sp.request_id = w.id
                              AND sp.generator_id = %s
                              AND sp.status = 'failed_transient'
                        )
                            THEN 'wanted_failed_transient'
                        ELSE 'wanted_no_plan'
                    END AS bucket
                FROM wanted w
                LEFT JOIN search_plans active_plan
                  ON active_plan.id = w.active_plan_id
            )
            SELECT
                COUNT(*)::int AS wanted_total,
                COUNT(*) FILTER (WHERE bucket = 'wanted_searchable')::int
                    AS wanted_searchable,
                COUNT(*) FILTER (WHERE bucket = 'wanted_legacy')::int
                    AS wanted_legacy,
                COUNT(*) FILTER (WHERE bucket = 'wanted_failed_deterministic')::int
                    AS wanted_failed_deterministic,
                COUNT(*) FILTER (WHERE bucket = 'wanted_failed_transient')::int
                    AS wanted_failed_transient,
                COUNT(*) FILTER (WHERE bucket = 'wanted_no_plan')::int
                    AS wanted_no_plan
            FROM classified
            """,
            (generator_id, generator_id, generator_id, generator_id),
        )
        fetched: _ReadinessBucketRow | None = cur.fetchone()
        row = fetched or _EMPTY_READINESS_BUCKET_ROW
        return {
            "generator_id": generator_id,
            "wanted_total": int(row.get("wanted_total") or 0),
            "wanted_searchable": int(row.get("wanted_searchable") or 0),
            "wanted_legacy": int(row.get("wanted_legacy") or 0),
            "wanted_failed_deterministic": int(
                row.get("wanted_failed_deterministic") or 0),
            "wanted_failed_transient": int(
                row.get("wanted_failed_transient") or 0),
            "wanted_no_plan": int(row.get("wanted_no_plan") or 0),
        }


    # ----------------------------------------------------------------
    # Persisted search plans
    # ----------------------------------------------------------------
    #
    # All plan DDL lives in migrations/014_persisted_search_plans.sql.
    # These methods read/write only -- never CREATE/ALTER. The
    # consumed-attempt method (`record_consumed_search_attempt`) is the
    # one intentional exception to PipelineDB's autocommit rule: it must
    # log + advance cursor in one transaction. See the method docstring.

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
        set_active: bool = True,
    ) -> int:
        """Create a successful plan + items; optionally make it the active
        plan and reset the request's cursor/cycle.

        Items must be non-empty (successful plans by contract carry at least
        one runnable slot); the CHECK + UNIQUE constraints in migration 014
        enforce non-empty queries and unique ``(plan, ordinal)``.

        Runs in a single transaction so the plan, its items, and the cursor
        update either all land or none do. Used by add-time generation,
        startup reconciliation, and explicit regeneration. Callers that need
        to supersede an existing active plan should call
        ``supersede_search_plan_with_replacement`` instead -- it takes the
        same shape and additionally flips the old active plan and updates
        the request cursor/cycle to point at the new one.
        """
        if not items:
            raise ValueError(
                "create_successful_search_plan requires at least one item; "
                "use create_failed_search_plan for empty results.")
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
                    (request_id,),
                )
                request_row = cur.fetchone()
                if request_row is None:
                    raise ValueError(f"request {request_id} not found")
                if request_row["status"] == "replaced":
                    raise ReplacedRequestMutationError(request_id)
                cur.execute(
                    """
                    INSERT INTO search_plans
                        (request_id, generator_id, status,
                         metadata_snapshot, provenance, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        generator_id,
                        PLAN_STATUS_ACTIVE,
                        _json_param(
                            metadata_snapshot, SearchPlanMetadataSnapshot),
                        _json_param(provenance, SearchPlanProvenance),
                        now,
                    ),
                )
                row = cur.fetchone()
                assert row is not None, "INSERT RETURNING must produce a row"
                plan_id = int(row["id"])

                pg_execute_values(
                    cur,
                    """
                    INSERT INTO search_plan_items
                        (plan_id, ordinal, strategy, query,
                         canonical_query_key, repeat_group, provenance)
                    VALUES %s
                    """,
                    [
                        (
                            plan_id,
                            item.ordinal,
                            item.strategy,
                            item.query,
                            item.canonical_query_key,
                            item.repeat_group,
                            _json_param(
                                item.provenance, SearchPlanItemProvenance),
                        )
                        for item in items
                    ],
                )

                if set_active:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET active_plan_id = %s,
                            next_plan_ordinal = 0,
                            plan_cycle_count = 0,
                            updated_at = %s
                        WHERE id = %s
                          AND status != 'replaced'
                        """,
                        (plan_id, now, request_id),
                    )
                    if cur.rowcount != 1:
                        raise ReplacedRequestMutationError(request_id)
            self.conn.commit()
            return plan_id


    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int:
        """Persist one generation failure attempt.

        ``transient=False`` -> deterministic sticky failure (no runnable
        query, missing required metadata): request stays wanted but is not
        searchable until a successful plan replaces it.

        ``transient=True`` -> retryable (resolver outage, etc.): startup
        reconciliation will retry on a later cycle.

        Either way, the request's existing active plan (if any) is left
        untouched -- failed regeneration must not disable a previously
        good plan.
        """
        status = (
            PLAN_STATUS_FAILED_TRANSIENT if transient
            else PLAN_STATUS_FAILED_DETERMINISTIC
        )
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT status FROM album_requests WHERE id = %s FOR UPDATE",
                    (request_id,),
                )
                request_row = cur.fetchone()
                if request_row is None:
                    raise ValueError(f"request {request_id} not found")
                if request_row["status"] == "replaced":
                    raise ReplacedRequestMutationError(request_id)
                cur.execute(
                    """
                    INSERT INTO search_plans
                        (request_id, generator_id, status, failure_class,
                         metadata_snapshot, provenance, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        generator_id,
                        status,
                        failure_class,
                        _json_param(
                            metadata_snapshot, SearchPlanMetadataSnapshot),
                        _json_param(provenance, SearchPlanProvenance),
                        error_message,
                    ),
                )
                row = cur.fetchone()
                assert row is not None, "INSERT RETURNING must produce a row"
                plan_id = int(row["id"])
            self.conn.commit()
            return plan_id


    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, object] | None = None,
        provenance: dict[str, object] | None = None,
    ) -> int:
        """Create a new successful plan AND replace the existing active plan
        for this request, atomically.

        The previous active plan (if any) is flipped to status='superseded'
        with ``superseded_at`` and ``superseded_by_plan_id`` populated. The
        request's cursor/cycle is reset to ``(0, 0)`` and ``active_plan_id``
        repointed at the new plan.

        Used by explicit regeneration and by startup reconciliation when an
        old-generator plan is being replaced. Falls back to
        ``create_successful_search_plan(set_active=True)`` semantics when
        the request has no active plan yet.
        """
        if not items:
            raise ValueError(
                "supersede_search_plan_with_replacement requires items.")
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # Read current active plan id under the lock implied by the
                # transaction; NULL means "no replacement, just create+activate".
                cur.execute(
                    "SELECT active_plan_id, status FROM album_requests WHERE id = %s "
                    "FOR UPDATE",
                    (request_id,),
                )
                req_row = cur.fetchone()
                if req_row is None:
                    raise ValueError(
                        f"request {request_id} not found")
                if req_row["status"] == "replaced":
                    raise ReplacedRequestMutationError(request_id)
                old_active_id = req_row["active_plan_id"]

                # Detach the old active plan first so the partial unique
                # index "one active per request" lets us insert the new
                # active row.
                if old_active_id is not None:
                    cur.execute(
                        """
                        UPDATE search_plans
                        SET status = %s,
                            superseded_at = %s
                        WHERE id = %s
                        """,
                        (PLAN_STATUS_SUPERSEDED, now, old_active_id),
                    )

                cur.execute(
                    """
                    INSERT INTO search_plans
                        (request_id, generator_id, status,
                         metadata_snapshot, provenance, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        request_id,
                        generator_id,
                        PLAN_STATUS_ACTIVE,
                        _json_param(
                            metadata_snapshot, SearchPlanMetadataSnapshot),
                        _json_param(provenance, SearchPlanProvenance),
                        now,
                    ),
                )
                new_row = cur.fetchone()
                assert new_row is not None
                new_plan_id = int(new_row["id"])

                if old_active_id is not None:
                    cur.execute(
                        """
                        UPDATE search_plans
                        SET superseded_by_plan_id = %s
                        WHERE id = %s
                        """,
                        (new_plan_id, old_active_id),
                    )

                pg_execute_values(
                    cur,
                    """
                    INSERT INTO search_plan_items
                        (plan_id, ordinal, strategy, query,
                         canonical_query_key, repeat_group, provenance)
                    VALUES %s
                    """,
                    [
                        (
                            new_plan_id,
                            item.ordinal,
                            item.strategy,
                            item.query,
                            item.canonical_query_key,
                            item.repeat_group,
                            _json_param(
                                item.provenance, SearchPlanItemProvenance),
                        )
                        for item in items
                    ],
                )

                cur.execute(
                    """
                    UPDATE album_requests
                    SET active_plan_id = %s,
                        next_plan_ordinal = 0,
                        plan_cycle_count = 0,
                        updated_at = %s
                    WHERE id = %s
                      AND status != 'replaced'
                    """,
                    (new_plan_id, now, request_id),
                )
                if cur.rowcount != 1:
                    raise ReplacedRequestMutationError(request_id)
            self.conn.commit()
            return new_plan_id


    def get_active_search_plan(
        self,
        request_id: int,
    ) -> ActiveSearchPlan | None:
        """Return the active plan + items + cursor state for one request.

        Returns ``None`` when the request has no active plan (either it was
        never generated, or the latest attempt failed deterministically).
        Use ``get_search_plan_inspection`` to also surface failed/superseded
        plans for human inspection.

        Single-query implementation: joins ``album_requests`` →
        ``search_plans`` → ``search_plan_items`` and aggregates items into
        a JSONB array via ``jsonb_agg(... ORDER BY spi.ordinal)
        FILTER (WHERE spi.id IS NOT NULL)``. The FILTER clause keeps the
        outer LEFT JOIN safe when a plan has zero items
        (``coalesce(..., '[]'::jsonb)`` returns an empty list rather than
        ``[null]``). Phase 2 calls this once per wanted album per cycle,
        so collapsing 2 RTTs to 1 saves ~1168 round-trips/cycle in prod.
        """
        cur = self._execute(
            """
            SELECT ar.next_plan_ordinal, ar.plan_cycle_count,
                   sp.id AS plan_id, sp.request_id, sp.generator_id,
                   sp.status, sp.failure_class, sp.metadata_snapshot,
                   sp.provenance, sp.error_message, sp.superseded_at,
                   sp.superseded_by_plan_id, sp.created_at,
                   COALESCE(
                     jsonb_agg(
                       jsonb_build_object(
                         'id', spi.id,
                         'plan_id', spi.plan_id,
                         'ordinal', spi.ordinal,
                         'strategy', spi.strategy,
                         'query', spi.query,
                         'canonical_query_key', spi.canonical_query_key,
                         'repeat_group', spi.repeat_group,
                         'provenance', spi.provenance
                       )
                       ORDER BY spi.ordinal ASC
                     ) FILTER (WHERE spi.id IS NOT NULL),
                     '[]'::jsonb
                   ) AS items_json
            FROM album_requests ar
            JOIN search_plans sp ON ar.active_plan_id = sp.id
            LEFT JOIN search_plan_items spi ON spi.plan_id = sp.id
            WHERE ar.id = %s
            GROUP BY ar.next_plan_ordinal, ar.plan_cycle_count,
                     sp.id, sp.request_id, sp.generator_id, sp.status,
                     sp.failure_class, sp.metadata_snapshot, sp.provenance,
                     sp.error_message, sp.superseded_at,
                     sp.superseded_by_plan_id, sp.created_at
            """,
            (request_id,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        plan = SearchPlanRow(
            id=int(row["plan_id"]),
            request_id=int(row["request_id"]),
            generator_id=row["generator_id"],
            status=row["status"],
            failure_class=row["failure_class"],
            metadata_snapshot=_metadata_snapshot_from_jsonb(
                row["metadata_snapshot"]),
            provenance=_plan_provenance_from_jsonb(row["provenance"]),
            error_message=row["error_message"],
            superseded_at=row["superseded_at"],
            superseded_by_plan_id=(
                int(row["superseded_by_plan_id"])
                if row["superseded_by_plan_id"] is not None else None),
            created_at=row["created_at"],
        )
        items = [
            SearchPlanItemRow(
                id=int(it["id"]),
                plan_id=int(it["plan_id"]),
                ordinal=int(it["ordinal"]),
                strategy=it["strategy"],
                query=it["query"],
                canonical_query_key=it["canonical_query_key"],
                repeat_group=it["repeat_group"],
                provenance=_item_provenance_from_jsonb(it["provenance"]),
            )
            for it in row["items_json"]
        ]
        return ActiveSearchPlan(
            plan=plan,
            items=items,
            next_ordinal=int(row["next_plan_ordinal"]),
            cycle_count=int(row["plan_cycle_count"]),
        )


    def advance_search_plan_cursor(
        self,
        request_id: int,
        *,
        target_ordinal: int,
        plan_item_count: int,
    ) -> tuple[int, int, int]:
        """Operator-driven forward-only advance of ``next_plan_ordinal``.

        Used by ``SearchPlanService.advance_for_request`` (via CLI / web
        API). Forward-only by design — backward intent should go through
        ``regenerate``. Returns ``(active_plan_id, previous_ordinal,
        new_ordinal)`` on success.

        Validates inside the row lock so concurrent
        ``record_consumed_search_attempt`` (executor cursor advance) can't
        race past the operator: SELECT ... FOR UPDATE pins ``album_requests``
        for the duration of the check + UPDATE.

        Raises ``ValueError`` for: missing request, no active plan,
        ``target_ordinal`` outside ``[0, plan_item_count)``, or
        ``target_ordinal <= current_ordinal`` (forward-only). Service-layer
        callers translate these into structured outcomes; the DB layer's
        job is just to keep the invariant.
        """
        if plan_item_count <= 0:
            raise ValueError(
                f"plan_item_count must be > 0 (got {plan_item_count})")
        if target_ordinal < 0 or target_ordinal >= plan_item_count:
            raise ValueError(
                f"target_ordinal {target_ordinal} out of range "
                f"[0, {plan_item_count})")
        with self._atomic():
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT active_plan_id, next_plan_ordinal, status "
                    "FROM album_requests WHERE id = %s FOR UPDATE",
                    (request_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"request {request_id} not found")
                if row["status"] == "replaced":
                    raise ReplacedRequestMutationError(request_id)
                active_plan_id = row["active_plan_id"]
                if active_plan_id is None:
                    raise ValueError(
                        f"request {request_id} has no active plan")
                previous_ordinal = int(row["next_plan_ordinal"])
                if target_ordinal <= previous_ordinal:
                    raise ValueError(
                        f"target_ordinal {target_ordinal} must be greater "
                        f"than current next_plan_ordinal {previous_ordinal} "
                        "(advance is forward-only; use regenerate for "
                        "backward intent)")
                cur.execute(
                    "UPDATE album_requests SET next_plan_ordinal = %s, "
                    "updated_at = NOW() WHERE id = %s "
                    "AND status != 'replaced'",
                    (target_ordinal, request_id),
                )
                if cur.rowcount != 1:
                    raise ReplacedRequestMutationError(request_id)
            self.conn.commit()
            return (int(active_plan_id), previous_ordinal, target_ordinal)


    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Return wanted rows whose active plan matches ``generator_id``.

        This is the **execution-eligibility** filter used by the Phase 2
        search loop. A wanted request is searchable only if:

          * ``status = 'wanted'`` (same as ``get_wanted``), and
          * ``next_retry_after`` is null or already due (same backoff
            semantics as ``get_wanted``), and
          * ``active_plan_id`` points at a row in ``search_plans`` whose
            ``status = 'active'`` AND ``generator_id = %s``, and
          * no YouTube download/import work conflicts with Soulseek, and
          * ``album_title`` contains none of ``title_blacklist``.

        A bounded result reserves a floor-rounded quarter of its page for
        rows whose immutable ``created_at`` is less than 24 hours old (4/12
        at production's page size 16). Each cohort is randomized, and unused
        capacity is borrowed in either direction. The shared ``eligible`` CTE
        applies every gate before cohort rank or capacity is assigned.

        Rows with no active plan, a deterministic-failed-only plan, a
        transient-failed-only plan, or an old-generator active plan are
        excluded -- startup reconciliation owns repairing those before
        the next cycle.

        Forensic / dashboard / inspection callers should keep using the
        older ``get_wanted`` (no plan filter) so they can show every
        wanted row regardless of plan readiness.
        """
        snapshot_at = now or datetime.now(timezone.utc)
        blacklist = [term for term in title_blacklist if term]
        eligible_cte = """
            WITH eligible AS MATERIALIZED (
              SELECT ar.*,
                     (ar.created_at
                        + %s * INTERVAL '1 hour' > %s) AS is_new_request
              FROM album_requests ar
              JOIN search_plans sp ON ar.active_plan_id = sp.id
            WHERE ar.status = 'wanted'
              AND (ar.next_retry_after IS NULL OR ar.next_retry_after <= %s)
              AND sp.status = 'active'
              AND sp.generator_id = %s
              AND NOT EXISTS (
                SELECT 1
                FROM download_log dl
                WHERE dl.request_id = ar.id
                  AND dl.source = 'youtube'
                  AND dl.outcome = 'youtube_running'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM import_jobs ij
                WHERE ij.request_id = ar.id
                  AND ij.job_type = %s
                  AND ij.status IN (
                      'queued', 'running', 'recovery_required'
                  )
              )
              AND NOT EXISTS (
                SELECT 1
                FROM unnest(%s::text[]) AS blocked(term)
                WHERE blocked.term <> ''
                  AND POSITION(
                    LOWER(blocked.term) IN LOWER(ar.album_title)
                  ) > 0
              )
            )
        """
        params: list[object] = [
            NEW_REQUEST_PRIORITY_HOURS,
            snapshot_at,
            snapshot_at,
            generator_id,
            IMPORT_JOB_YOUTUBE,
            blacklist,
        ]
        if limit is None:
            sql = eligible_cte + """
                SELECT ar.*
                FROM album_requests ar
                JOIN eligible e ON e.id = ar.id
                ORDER BY RANDOM()
            """
        else:
            page_size = int(limit)
            slots = search_cohort_slots(page_size)
            sql = eligible_cte + """
                , ranked AS (
                  SELECT id,
                         is_new_request,
                         ROW_NUMBER() OVER (
                           PARTITION BY is_new_request
                           ORDER BY RANDOM()
                         ) AS cohort_rank
                  FROM eligible
                ), selected_ids AS (
                  SELECT id
                  FROM ranked
                  ORDER BY
                    CASE
                      WHEN (
                        is_new_request AND cohort_rank <= %s
                      ) OR (
                        NOT is_new_request AND cohort_rank <= %s
                      ) THEN 0
                      ELSE 1
                    END,
                    RANDOM()
                  LIMIT %s
                )
                SELECT ar.*
                FROM album_requests ar
                JOIN selected_ids selected ON selected.id = ar.id
                ORDER BY RANDOM()
            """
            params.extend((slots.new, slots.established, page_size))
        cur = self._execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]


    def list_wanted_for_plan_reconciliation(
        self,
    ) -> list[WantedReconciliationCandidate]:
        """All-wanted scan for startup reconciliation.

        Ignores ``next_retry_after`` and the page-size limit that
        ``get_wanted`` applies. Used once per startup to decide which
        wanted rows need a generated/regenerated plan -- callers must
        compare ``active_plan_generator_id`` to the current generator id
        themselves.
        """
        cur = self._execute(
            """
            SELECT ar.id AS request_id,
                   CASE
                     WHEN sp.status = 'active' THEN ar.active_plan_id
                     ELSE NULL
                   END AS active_plan_id,
                   ar.next_plan_ordinal, ar.plan_cycle_count,
                   CASE
                     WHEN sp.status = 'active' THEN sp.generator_id
                     ELSE NULL
                   END AS active_plan_generator_id
            FROM album_requests ar
            LEFT JOIN search_plans sp ON ar.active_plan_id = sp.id
            WHERE ar.status = 'wanted'
            ORDER BY ar.id
            """
        )
        return [
            WantedReconciliationCandidate(
                request_id=int(r["request_id"]),
                active_plan_id=(
                    int(r["active_plan_id"])
                    if r["active_plan_id"] is not None else None),
                active_plan_generator_id=r["active_plan_generator_id"],
                next_plan_ordinal=int(r["next_plan_ordinal"]),
                plan_cycle_count=int(r["plan_cycle_count"]),
            )
            for r in cur.fetchall()
        ]


    def list_search_plan_classification_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, DryRunPlanClassification]:
        """Batch-fetch the per-request data dry-run classification needs.

        Replaces the per-row ``get_search_plan_inspection`` call inside
        ``startup_reconciliation._classify_dry_run`` (5 sequential
        queries × ~600 candidates ≈ 2,920 round-trips) with a single
        query.

        Returns one entry per request id passed in. Requests without
        any failed plan rows still get an entry whose generator-id
        fields are both ``None``. Requests not in ``request_ids`` are
        absent from the result. An empty input list returns ``{}``
        without hitting the DB.

        We use ``DISTINCT ON (request_id, status)`` ordered by
        ``created_at DESC, id DESC`` so each request gets at most one
        row per failure status -- the same row ``_latest()`` selects
        inside ``get_search_plan_inspection``.
        """
        if not request_ids:
            return {}
        # Initialise every requested id with a None/None entry so
        # callers don't have to handle "missing" vs. "no failed plan".
        out: dict[int, DryRunPlanClassification] = {
            int(rid): DryRunPlanClassification(
                request_id=int(rid),
                latest_failed_deterministic_generator_id=None,
                latest_failed_transient_generator_id=None,
            )
            for rid in request_ids
        }
        cur = self._execute(
            """
            SELECT DISTINCT ON (request_id, status)
                   request_id, status, generator_id, created_at
            FROM search_plans
            WHERE request_id = ANY(%s)
              AND status IN (%s, %s)
            ORDER BY request_id, status, created_at DESC, id DESC
            """,
            (
                list(out.keys()),
                PLAN_STATUS_FAILED_DETERMINISTIC,
                PLAN_STATUS_FAILED_TRANSIENT,
            ),
        )
        for r in cur.fetchall():
            rid = int(r["request_id"])
            current = out[rid]
            if r["status"] == PLAN_STATUS_FAILED_DETERMINISTIC:
                out[rid] = DryRunPlanClassification(
                    request_id=rid,
                    latest_failed_deterministic_generator_id=r[
                        "generator_id"],
                    latest_failed_transient_generator_id=current
                        .latest_failed_transient_generator_id,
                    latest_failed_transient_created_at=current
                        .latest_failed_transient_created_at,
                )
            elif r["status"] == PLAN_STATUS_FAILED_TRANSIENT:
                out[rid] = DryRunPlanClassification(
                    request_id=rid,
                    latest_failed_deterministic_generator_id=current
                        .latest_failed_deterministic_generator_id,
                    latest_failed_transient_generator_id=r[
                        "generator_id"],
                    latest_failed_transient_created_at=r["created_at"],
                )
        return out


    def get_search_plan_inspection(
        self,
        request_id: int,
    ) -> SearchPlanInspection:
        """Aggregate read for CLI/API inspection.

        Returns the active plan (with items + cursor), the latest
        deterministic and transient failed attempts (most recent of each),
        the count of superseded plans, and the count of historical
        search_log rows for this request that pre-date persisted plans.
        """
        active = self.get_active_search_plan(request_id)

        def _latest(status: str) -> SearchPlanRow | None:
            cur = self._execute(
                """
                SELECT id, request_id, generator_id, status, failure_class,
                       metadata_snapshot, provenance, error_message,
                       superseded_at, superseded_by_plan_id, created_at
                FROM search_plans
                WHERE request_id = %s AND status = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (request_id, status),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return SearchPlanRow(
                id=int(row["id"]),
                request_id=int(row["request_id"]),
                generator_id=row["generator_id"],
                status=row["status"],
                failure_class=row["failure_class"],
                metadata_snapshot=_metadata_snapshot_from_jsonb(
                    row["metadata_snapshot"]),
                provenance=_plan_provenance_from_jsonb(row["provenance"]),
                error_message=row["error_message"],
                superseded_at=row["superseded_at"],
                superseded_by_plan_id=(
                    int(row["superseded_by_plan_id"])
                    if row["superseded_by_plan_id"] is not None else None),
                created_at=row["created_at"],
            )

        latest_det = _latest(PLAN_STATUS_FAILED_DETERMINISTIC)
        latest_trans = _latest(PLAN_STATUS_FAILED_TRANSIENT)

        sup_cur = self._execute(
            "SELECT COUNT(*) AS c FROM search_plans "
            "WHERE request_id = %s AND status = %s",
            (request_id, PLAN_STATUS_SUPERSEDED),
        )
        sup_row = sup_cur.fetchone()
        superseded_count = int(sup_row["c"]) if sup_row is not None else 0

        legacy_cur = self._execute(
            "SELECT COUNT(*) AS c FROM search_log "
            "WHERE request_id = %s AND plan_id IS NULL",
            (request_id,),
        )
        legacy_row = legacy_cur.fetchone()
        legacy_count = int(legacy_row["c"]) if legacy_row is not None else 0

        return SearchPlanInspection(
            request_id=request_id,
            active=active,
            latest_failed_deterministic=latest_det,
            latest_failed_transient=latest_trans,
            superseded_count=superseded_count,
            legacy_search_log_count=legacy_count,
        )


    def get_search_plan_stats(
        self,
        request_id: int,
        *,
        current_only: bool = True,
        prefetched_history: list[dict[str, object]] | None = None,
    ) -> SearchPlanStats:
        """Aggregate plan-aware ``search_log`` rows into usefulness stats.

        Two grouping levels per cohort:
          * **slots** keyed by ``(plan_id, ordinal, strategy)`` —
            ordinal-ordered.
          * **query_groups** keyed by ``(plan_id, repeat_group,
            canonical_query_key)`` — stable order by
            ``(repeat_group, canonical_query_key)``.

        ``current_only=True`` (default) returns the active-plan cohort
        in ``current`` and an empty ``superseded_and_legacy`` cohort.
        ``current_only=False`` populates both cohorts from every plan
        the request ever had plus a ``legacy_bucket`` for pre-plan rows.

        Cache attribution is reported as ``cycle_only`` because
        ``search_log`` has no per-search cache columns today (cache
        counters live on ``cycle_metrics`` — see
        ``migrations/011_cycle_metrics.sql``). If a future migration
        adds them, flip ``cache_per_search_available=True`` here.
        """
        active = self.get_active_search_plan(request_id)
        active_plan_id = active.plan.id if active is not None else None

        history = (prefetched_history if prefetched_history is not None
                   else self.get_search_history(request_id))

        plan_aware = [r for r in history if r.get("plan_id") is not None]
        legacy = [r for r in history if r.get("plan_id") is None]

        current_rows = (
            [r for r in plan_aware if r.get("plan_id") == active_plan_id]
            if active_plan_id is not None else []
        )
        if current_only:
            other_rows: list[dict[str, object]] = []
            other_legacy: list[dict[str, object]] = []
        else:
            other_rows = [r for r in plan_aware
                          if r.get("plan_id") != active_plan_id]
            other_legacy = legacy

        current_bucket = _build_stats_bucket(
            plan_aware_rows=current_rows, legacy_rows=[],
            include_legacy_bucket=False,
        )
        other_bucket = _build_stats_bucket(
            plan_aware_rows=other_rows, legacy_rows=other_legacy,
            include_legacy_bucket=True,
        )
        return SearchPlanStats(
            request_id=request_id,
            current=current_bucket,
            superseded_and_legacy=other_bucket,
        )


    def record_consumed_search_attempt(
        self,
        attempt: ConsumedAttemptInput,
    ) -> ConsumedAttemptResult:
        """Atomically log a consumed search attempt and advance/wrap cursor.

        This is the one intentional exception to PipelineDB's
        ``autocommit=True`` rule. The transaction does:

          1. Re-read the request's ``active_plan_id`` and
             ``next_plan_ordinal`` ``FOR UPDATE``.
          2. Insert one ``search_log`` row carrying full plan context and
             a ``plan_cycle_snapshot``.
          3. If the executing plan/ordinal still match the active state:
             advance ordinal (or wrap to 0 + cycle++) and stamp
             ``cursor_update_status`` accordingly; flagged ``advanced``
             or ``wrapped`` on the log row.
          4. Otherwise: leave the cursor alone, flag the log row as
             ``stale`` with ``stale_reason='regenerated'`` (or
             ``'request_replaced'`` for a terminal ancestor) and
             ``execution_stage='stale_completion'``.

        Either every write commits or none do. Callers must NOT separately
        call ``log_search`` for the same accepted attempt -- this method
        is the consumed-attempt seam.

        ``apply_scheduler_attempt=True`` increments ``search_attempts`` and
        sets backoff inside the same transaction, so the legacy
        ``search_attempts`` field stays a scheduler-only counter.
        """
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT active_plan_id, next_plan_ordinal, "
                    "       plan_cycle_count, status "
                    "FROM album_requests WHERE id = %s FOR UPDATE",
                    (attempt.request_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(
                        f"request {attempt.request_id} not found")
                active_plan_id = row["active_plan_id"]
                next_ordinal = int(row["next_plan_ordinal"])
                cycle_count = int(row["plan_cycle_count"])
                current_status = str(row["status"])

                cur.execute(
                    """
                    SELECT 1
                    FROM search_plan_items spi
                    JOIN search_plans sp ON sp.id = spi.plan_id
                    WHERE spi.id = %s
                      AND spi.plan_id = %s
                      AND sp.request_id = %s
                    """,
                    (
                        attempt.plan_item_id,
                        attempt.plan_id,
                        attempt.request_id,
                    ),
                )
                if cur.fetchone() is None:
                    raise ValueError(
                        f"plan_item_id={attempt.plan_item_id} does not "
                        f"belong to plan_id={attempt.plan_id} for "
                        f"request_id={attempt.request_id}")

                is_stale = (
                    current_status == "replaced"
                    or active_plan_id != attempt.plan_id
                    or next_ordinal != attempt.plan_ordinal
                    or cycle_count != attempt.cycle_count_snapshot
                )

                if is_stale:
                    cursor_update_status = CURSOR_UPDATE_STALE
                    execution_stage = SEARCH_LOG_STAGE_STALE_COMPLETION
                    stale_reason = (
                        "request_replaced"
                        if current_status == "replaced"
                        else "regenerated"
                    )
                    new_next_ordinal = next_ordinal
                    new_cycle = cycle_count
                else:
                    execution_stage = SEARCH_LOG_STAGE_ACCEPTED
                    stale_reason = None
                    plan_item_count = max(int(attempt.plan_item_count), 0)
                    if plan_item_count == 0:
                        # Pathological: caller said no items. Treat as
                        # advanced-without-wrap to avoid /0 wrap math; the
                        # generator's CHECK + service contract should
                        # prevent this in practice.
                        cursor_update_status = CURSOR_UPDATE_ADVANCED
                        new_next_ordinal = next_ordinal + 1
                        new_cycle = cycle_count
                    elif attempt.plan_ordinal >= plan_item_count - 1:
                        cursor_update_status = CURSOR_UPDATE_WRAPPED
                        new_next_ordinal = 0
                        new_cycle = cycle_count + 1
                    else:
                        cursor_update_status = CURSOR_UPDATE_ADVANCED
                        new_next_ordinal = next_ordinal + 1
                        new_cycle = cycle_count

                cur.execute(
                    """
                    INSERT INTO search_log (
                        request_id, query, result_count, elapsed_s, outcome,
                        candidates, variant, final_state,
                        browse_time_s, match_time_s,
                        peers_browsed, peers_browsed_lazy, fanout_waves,
                        plan_id, plan_item_id, plan_ordinal,
                        plan_strategy, plan_canonical_query_key,
                        plan_repeat_group, plan_generator_id,
                        execution_stage, attempt_consumed,
                        cursor_update_status, stale_reason,
                        plan_cycle_snapshot,
                        pre_filter_skip_count,
                        rejection_reason, result_count_uncapped,
                        query_token_count, query_distinct_token_count,
                        expected_track_count, matcher_score_top1,
                        query_template
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s
                    )
                    RETURNING id
                    """,
                    (
                        attempt.request_id,
                        attempt.query,
                        attempt.result_count,
                        attempt.elapsed_s,
                        attempt.outcome,
                        attempt.candidates_json,
                        attempt.variant,
                        attempt.final_state,
                        attempt.browse_time_s,
                        attempt.match_time_s,
                        attempt.peers_browsed,
                        attempt.peers_browsed_lazy,
                        attempt.fanout_waves,
                        attempt.plan_id,
                        attempt.plan_item_id,
                        attempt.plan_ordinal,
                        attempt.plan_strategy,
                        attempt.plan_canonical_query_key,
                        attempt.plan_repeat_group,
                        attempt.plan_generator_id,
                        execution_stage,
                        not is_stale,
                        cursor_update_status,
                        stale_reason,
                        attempt.cycle_count_snapshot,
                        attempt.pre_filter_skip_count,
                        attempt.rejection_reason,
                        attempt.result_count_uncapped,
                        attempt.query_token_count,
                        attempt.query_distinct_token_count,
                        attempt.expected_track_count,
                        attempt.matcher_score_top1,
                        attempt.query_template,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None
                search_log_id = int(log_row["id"])

                # Cursor + scheduler/backoff writes only when not stale.
                if not is_stale:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET next_plan_ordinal = %s,
                            plan_cycle_count = %s,
                            updated_at = %s
                        WHERE id = %s
                          AND status != 'replaced'
                        """,
                        (new_next_ordinal, new_cycle, now,
                         attempt.request_id),
                    )

                    if (
                        attempt.apply_scheduler_attempt
                        and not attempt.scheduler_success
                    ):
                        cur.execute(
                            """
                            UPDATE album_requests
                            SET search_attempts = COALESCE(search_attempts, 0) + 1,
                                last_attempt_at = %s,
                                updated_at = %s
                            WHERE id = %s
                              AND status != 'replaced'
                            RETURNING search_attempts
                            """,
                            (now, now, attempt.request_id),
                        )
                        s_row = cur.fetchone()
                        assert s_row is not None
                        new_count = int(s_row["search_attempts"])
                        backoff_minutes = min(
                            BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                            BACKOFF_MAX_MINUTES,
                        )
                        cur.execute(
                            "UPDATE album_requests "
                            "SET next_retry_after = %s WHERE id = %s "
                            "AND status != 'replaced'",
                            (now + timedelta(minutes=backoff_minutes),
                             attempt.request_id),
                        )
                    elif (
                        attempt.apply_scheduler_attempt
                        and attempt.scheduler_success
                    ):
                        # Reset retry-pacing on a useful slot. We do not
                        # reset attempt counters -- those are forensic.
                        cur.execute(
                            "UPDATE album_requests "
                            "SET last_attempt_at = %s, updated_at = %s "
                            "WHERE id = %s AND status != 'replaced'",
                            (now, now, attempt.request_id),
                        )

                    # U12: when the cursor just wrapped, classify the
                    # cycle that completed (cycle_count, pre-increment)
                    # and persist the verdict to
                    # ``album_requests.failure_class``. Folded into
                    # this transaction rather than a separate
                    # ``update_failure_class`` call so the wrap and the
                    # classification commit together — operators
                    # cannot observe a cursor-advanced-but-unclassified
                    # state, nor a classified-but-unwrapped state. The
                    # classifier returns ``None`` for "no signal"
                    # (degenerate cycle with zero consumed attempts);
                    # in that case we leave the column alone so an
                    # earlier verdict survives.
                    if cursor_update_status == CURSOR_UPDATE_WRAPPED:
                        cur.execute(
                            """
                            SELECT outcome, rejection_reason
                            FROM search_log
                            WHERE request_id = %s
                              AND plan_cycle_snapshot = %s
                              AND attempt_consumed = TRUE
                            ORDER BY id
                            """,
                            (attempt.request_id, cycle_count),
                        )
                        summary_rows = cur.fetchall()
                        summaries = [
                            _SearchSummary(
                                outcome=str(r["outcome"]),
                                rejection_reason=(
                                    str(r["rejection_reason"])
                                    if r["rejection_reason"] is not None
                                    else None
                                ),
                            )
                            for r in summary_rows
                        ]
                        verdict = _classify_failure_class(
                            summaries, current_status=current_status,
                        )
                        if verdict is not None:
                            cur.execute(
                                "UPDATE album_requests "
                                "SET failure_class = %s, updated_at = %s "
                                "WHERE id = %s AND status != 'replaced'",
                                (verdict, now, attempt.request_id),
                            )

            self.conn.commit()
            return ConsumedAttemptResult(
                search_log_id=search_log_id,
                cursor_update_status=cursor_update_status,
                new_next_ordinal=new_next_ordinal,
                new_cycle_count=new_cycle,
                is_stale=is_stale,
            )


    def record_non_consuming_search_attempt(
        self,
        attempt: NonConsumingAttemptInput,
    ) -> int:
        """Record a pre-attempt / setup-failure search_log row.

        Always non-consuming -- cursor and cycle are never touched. Plan
        context fields are nullable because the failure may have happened
        before the executor resolved a plan/item. When
        ``apply_scheduler_attempt=True`` this also increments
        ``search_attempts`` and applies exponential backoff so a stuck
        request cannot spin.

        Returns the new ``search_log.id``.
        """
        with self._atomic():
            now = datetime.now(timezone.utc)
            with self.conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                cur.execute(
                    "SELECT plan_cycle_count, status "
                    "FROM album_requests WHERE id = %s FOR UPDATE",
                    (attempt.request_id,),
                )
                row = cur.fetchone()
                if row is None:
                    raise ValueError(
                        f"request {attempt.request_id} not found")
                cycle_snapshot = int(row["plan_cycle_count"])
                request_replaced = row["status"] == "replaced"

                cur.execute(
                    """
                    INSERT INTO search_log (
                        request_id, query, result_count, elapsed_s, outcome,
                        final_state,
                        plan_id, plan_item_id, plan_ordinal,
                        plan_strategy, plan_canonical_query_key,
                        plan_repeat_group, plan_generator_id,
                        execution_stage, attempt_consumed,
                        cursor_update_status, plan_cycle_snapshot,
                        pre_filter_skip_count,
                        rejection_reason, result_count_uncapped,
                        query_token_count, query_distinct_token_count,
                        expected_track_count, matcher_score_top1,
                        query_template
                    ) VALUES (
                        %s, %s, %s, %s, %s,
                        %s,
                        %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s,
                        %s, %s,
                        %s, %s,
                        %s, %s,
                        %s
                    )
                    RETURNING id
                    """,
                    (
                        attempt.request_id,
                        attempt.query,
                        attempt.result_count,
                        attempt.elapsed_s,
                        attempt.outcome,
                        attempt.final_state,
                        attempt.plan_id,
                        attempt.plan_item_id,
                        attempt.plan_ordinal,
                        attempt.plan_strategy,
                        attempt.plan_canonical_query_key,
                        attempt.plan_repeat_group,
                        attempt.plan_generator_id,
                        SEARCH_LOG_STAGE_PRE_ATTEMPT,
                        False,  # attempt_consumed
                        CURSOR_UPDATE_UNCHANGED,
                        cycle_snapshot,
                        attempt.pre_filter_skip_count,
                        attempt.rejection_reason,
                        attempt.result_count_uncapped,
                        attempt.query_token_count,
                        attempt.query_distinct_token_count,
                        attempt.expected_track_count,
                        attempt.matcher_score_top1,
                        attempt.query_template,
                    ),
                )
                log_row = cur.fetchone()
                assert log_row is not None
                search_log_id = int(log_row["id"])

                if attempt.apply_scheduler_attempt and not request_replaced:
                    cur.execute(
                        """
                        UPDATE album_requests
                        SET search_attempts = COALESCE(search_attempts, 0) + 1,
                            last_attempt_at = %s,
                            updated_at = %s
                        WHERE id = %s
                          AND status != 'replaced'
                        RETURNING search_attempts
                        """,
                        (now, now, attempt.request_id),
                    )
                    s_row = cur.fetchone()
                    assert s_row is not None
                    new_count = int(s_row["search_attempts"])
                    backoff_minutes = min(
                        BACKOFF_BASE_MINUTES * (2 ** (new_count - 1)),
                        BACKOFF_MAX_MINUTES,
                    )
                    cur.execute(
                        "UPDATE album_requests "
                        "SET next_retry_after = %s WHERE id = %s "
                        "AND status != 'replaced'",
                        (now + timedelta(minutes=backoff_minutes),
                         attempt.request_id),
                    )
            self.conn.commit()
            return search_log_id


    def get_search_summaries_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """Bulk-fetch rows from the ``request_search_summary`` view.

        Returns ``{request_id: row_dict}``. Requests with zero searches
        in the view's 14-day window are absent from the result; the
        triage composer renders the all-zeros summary in that case.
        """
        if not request_ids:
            return {}
        cur = self._execute(
            """
            SELECT request_id, total_searches, with_cands_count,
                   found_count, near_cap_count, zero_results_count,
                   pre_filter_skips_total, first_strategy_with_cands,
                   dominant_rejection_reason, last_search_at
            FROM request_search_summary
            WHERE request_id = ANY(%s)
            """,
            ([int(r) for r in request_ids],),
        )
        out: dict[int, dict[str, Any]] = {}
        for row in cur.fetchall():
            rid = int(row["request_id"])
            out[rid] = dict(row)
        return out


    def get_recent_search_log_for_requests(
        self,
        request_ids: list[int],
        *,
        per_request_limit: int,
    ) -> dict[int, list[dict[str, Any]]]:
        """Bulk-fetch the most-recent N ``search_log`` rows per request.

        Returns ``{request_id: [row, ...]}`` newest-first; each list is
        at most ``per_request_limit`` rows long. Uses a single
        ``ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY created_at
        DESC)`` window so the bulk path stays one query regardless of
        how many requests are in the cohort.

        Rows carry the columns the triage forensics struct needs —
        id, created_at, plan_strategy, query, outcome, result_count,
        rejection_reason, matcher_score_top1. Anything else stays in
        ``search_log``; ``search-plan history`` is the full-row view.
        """
        if not request_ids:
            return {}
        cur = self._execute(
            """
            SELECT id, request_id, created_at, plan_strategy, query,
                   outcome, result_count, rejection_reason,
                   matcher_score_top1
            FROM (
                SELECT sl.id, sl.request_id, sl.created_at,
                       sl.plan_strategy, sl.query, sl.outcome,
                       sl.result_count, sl.rejection_reason,
                       sl.matcher_score_top1,
                       ROW_NUMBER() OVER (
                           PARTITION BY sl.request_id
                           ORDER BY sl.created_at DESC, sl.id DESC
                       ) AS rn
                FROM search_log sl
                WHERE sl.request_id = ANY(%s)
            ) ranked
            WHERE rn <= %s
            ORDER BY request_id, created_at DESC, id DESC
            """,
            (
                [int(r) for r in request_ids],
                int(per_request_limit),
            ),
        )
        out: dict[int, list[dict[str, Any]]] = {}
        for row in cur.fetchall():
            rid = int(row["request_id"])
            out.setdefault(rid, []).append(dict(row))
        return out
