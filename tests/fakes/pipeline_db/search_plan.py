"""FakePipelineDB search_plan cluster — mirrors ``lib/pipeline_db/search_plan.py``.

Persisted search plans, plan items, and the cursor.
"""
from __future__ import annotations

import copy
from collections.abc import (
    Sequence,
)
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import (
    TYPE_CHECKING,
    Any,
)

import msgspec

if TYPE_CHECKING:
    from lib.pipeline_db import (
        SaturationSummary,
        SearchLogHistoryPage,
    )
    from lib.quality import CandidateScore
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_YOUTUBE,
)
from lib.pipeline_db import (
    CURSOR_UPDATE_UNCHANGED,
    CURSOR_UPDATE_WRAPPED,
    PLAN_STATUS_ACTIVE,
    PLAN_STATUS_FAILED_DETERMINISTIC,
    PLAN_STATUS_FAILED_TRANSIENT,
    PLAN_STATUS_SUPERSEDED,
    SEARCH_LOG_STAGE_PRE_ATTEMPT,
    ActiveSearchPlan,
    ConsumedAttemptInput,
    ConsumedAttemptResult,
    DryRunPlanClassification,
    NonConsumingAttemptInput,
    ReplacedRequestMutationError,
    SearchPlanInspection,
    SearchPlanItemInput,
    SearchPlanItemProvenance,
    SearchPlanItemRow,
    SearchPlanMetadataSnapshot,
    SearchPlanProvenance,
    SearchPlanRow,
    WantedReconciliationCandidate,
)
from lib.pipeline_db.decisions import (
    PLAN_READINESS_BUCKETS,
    classify_plan_readiness_bucket,
    cursor_advance_decision,
    saturation_summary_from_counts,
    search_backoff_minutes,
)
from lib.search_classification import (
    SearchSummary as _SearchSummary,
)
from lib.search_classification import (
    classify_failure_class as _classify_failure_class,
)
from lib.search_scheduler import (
    NEW_REQUEST_PRIORITY_HOURS,
    search_cohort_slots,
)
from tests.fakes._shared import _as_datetime, _utcnow
from tests.fakes.pipeline_db._base import _FakePipelineDBBase
from tests.fakes.pipeline_db._shared import _jsonb_column
from tests.fakes.rows import (
    SearchLogRow,
)


@dataclass
class _FakeSearchPlanRow:
    """In-memory mirror of a search_plans row."""
    id: int
    request_id: int
    generator_id: str
    status: str
    failure_class: str | None = None
    metadata_snapshot: dict[str, Any] | None = None
    provenance: dict[str, Any] | None = None
    error_message: str | None = None
    superseded_at: datetime | None = None
    superseded_by_plan_id: int | None = None
    created_at: datetime = field(default_factory=_utcnow)



@dataclass
class _FakeSearchPlanItemRow:
    """In-memory mirror of a search_plan_items row."""
    id: int
    plan_id: int
    ordinal: int
    strategy: str
    query: str
    canonical_query_key: str | None = None
    repeat_group: str | None = None
    provenance: dict[str, Any] | None = None


class _FakeSearchPlanMixin(_FakePipelineDBBase):
    """Persisted search plans, plan items, and the cursor."""

    # Production's ``get_search_plan_stats_history`` SELECTs a NARROW,
    # hand-listed column set (search_plan.py) — it deliberately excludes
    # ``candidates`` AND the U11 forensics columns (pre_filter_skip_count,
    # rejection_reason, result_count_uncapped, query_token_count,
    # query_distinct_token_count, expected_track_count, matcher_score_top1,
    # query_template) so inspection stats don't drag the wide row. The
    # fake MUST mirror that exact projection (#546 W1 parity).
    _SEARCH_PLAN_STATS_HISTORY_KEYS: tuple[str, ...] = (
        "id", "request_id", "query", "result_count", "elapsed_s", "outcome",
        "variant", "final_state", "browse_time_s", "match_time_s",
        "peers_browsed", "peers_browsed_lazy", "fanout_waves",
        "plan_id", "plan_item_id", "plan_ordinal", "plan_strategy",
        "plan_canonical_query_key", "plan_repeat_group",
        "plan_generator_id", "execution_stage", "attempt_consumed",
        "cursor_update_status", "stale_reason", "plan_cycle_snapshot",
        "created_at",
    )


    @staticmethod
    def _search_log_to_dict(entry: SearchLogRow) -> dict[str, object]:
        # Match production JSONB read behaviour: psycopg2 deserializes
        # ``search_log.candidates`` (JSONB) into a Python list/dict on
        # ``SELECT *``. The fake stores the encoded JSON string, so decode
        # here so consumers (e.g. the U7 web route + CLI) see the same
        # parsed-list shape they get from the real DB. Same job, same
        # column class, one helper (issue #1278 item 7, reader F4).
        candidates = _jsonb_column(entry.candidates)
        return {
            "id": entry.id,
            "request_id": entry.request_id,
            "query": entry.query,
            "result_count": entry.result_count,
            "elapsed_s": entry.elapsed_s,
            "outcome": entry.outcome,
            "created_at": entry.created_at,
            "candidates": candidates,
            "variant": entry.variant,
            "final_state": entry.final_state,
            "browse_time_s": entry.browse_time_s,
            "match_time_s": entry.match_time_s,
            "peers_browsed": entry.peers_browsed,
            "peers_browsed_lazy": entry.peers_browsed_lazy,
            "fanout_waves": entry.fanout_waves,
            # U1 plan-context fields. Mirror the real DB SELECT shape -- a
            # historical row writes through ``log_search`` keeps these as
            # None so legacy tests stay green.
            "plan_id": entry.plan_id,
            "plan_item_id": entry.plan_item_id,
            "plan_ordinal": entry.plan_ordinal,
            "plan_strategy": entry.plan_strategy,
            "plan_canonical_query_key": entry.plan_canonical_query_key,
            "plan_repeat_group": entry.plan_repeat_group,
            "plan_generator_id": entry.plan_generator_id,
            "execution_stage": entry.execution_stage,
            "attempt_consumed": entry.attempt_consumed,
            "cursor_update_status": entry.cursor_update_status,
            "stale_reason": entry.stale_reason,
            "plan_cycle_snapshot": entry.plan_cycle_snapshot,
            "pre_filter_skip_count": entry.pre_filter_skip_count,
            # U11 forensics columns. Same NULL semantics as production.
            "rejection_reason": entry.rejection_reason,
            "result_count_uncapped": entry.result_count_uncapped,
            "query_token_count": entry.query_token_count,
            "query_distinct_token_count": entry.query_distinct_token_count,
            "expected_track_count": entry.expected_track_count,
            "matcher_score_top1": entry.matcher_score_top1,
            "query_template": entry.query_template,
            "cross_request_conflict_request_ids": (
                entry.cross_request_conflict_request_ids),
        }

    def get_search_summaries_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, dict[str, Any]]:
        """In-memory mirror of ``PipelineDB.get_search_summaries_for_requests``.

        Aggregates ``self.search_logs`` against the same shape the
        production view emits. Requests with zero rows in window are
        omitted (the view excludes empty groups via ``GROUP BY``).
        """
        self.query_counts["get_search_summaries_for_requests"] = (
            self.query_counts.get("get_search_summaries_for_requests", 0) + 1
        )
        out: dict[int, dict[str, Any]] = {}
        for rid in request_ids:
            summary = self._compute_search_summary(int(rid))
            if summary is not None:
                out[int(rid)] = summary
        return out

    def get_recent_search_log_for_requests(
        self,
        request_ids: list[int],
        *,
        per_request_limit: int,
    ) -> dict[int, list[dict[str, Any]]]:
        """In-memory mirror of ``PipelineDB.get_recent_search_log_for_requests``.

        Walks ``self.search_logs`` newest-first and emits at most
        ``per_request_limit`` rows per request id.
        """
        self.query_counts["get_recent_search_log_for_requests"] = (
            self.query_counts.get("get_recent_search_log_for_requests", 0) + 1
        )
        wanted = {int(r) for r in request_ids}
        out: dict[int, list[dict[str, Any]]] = {}
        # Sort by (created_at, id) DESC so the most recent rows come
        # first per request.
        for entry in sorted(
            self.search_logs,
            key=lambda e: (e.created_at, e.id),
            reverse=True,
        ):
            if entry.request_id not in wanted:
                continue
            bucket = out.setdefault(entry.request_id, [])
            if len(bucket) >= int(per_request_limit):
                continue
            bucket.append({
                "id": entry.id,
                "request_id": entry.request_id,
                "created_at": entry.created_at,
                "plan_strategy": entry.plan_strategy,
                "query": entry.query,
                "outcome": entry.outcome,
                "result_count": entry.result_count,
                "rejection_reason": entry.rejection_reason,
                "matcher_score_top1": entry.matcher_score_top1,
                "cross_request_conflict_request_ids": (
                    entry.cross_request_conflict_request_ids),
            })
        return out


    def get_search_plan_readiness(
        self,
        generator_id: str,
    ) -> dict[str, Any]:
        """Mirror of ``PipelineDB.get_search_plan_readiness`` for tests.

        Walks ``self._requests`` + ``self.search_plans`` to gather each
        wanted row's three facts, then hands them to the ONE bucket rule
        (``decisions.classify_plan_readiness_bucket``) production's SQL
        CASE ladder mirrors. Only the gathering differs; the precedence —
        including "an active-plan pointer that resolves to nothing is not
        legacy" — is no longer restated here.
        """
        counts = dict.fromkeys(PLAN_READINESS_BUCKETS, 0)
        wanted_total = 0
        for req in self._requests.values():
            if req.get("status") != "wanted":
                continue
            wanted_total += 1
            active_id = req.get("active_plan_id")
            active_plan = (
                self.search_plans.get(active_id)
                if active_id is not None else None
            )
            req_id = req["id"]
            bucket = classify_plan_readiness_bucket(
                active_plan_generator_id=(
                    active_plan.generator_id
                    if active_plan is not None else None
                ),
                current_generator_id=generator_id,
                has_failed_deterministic=any(
                    p.request_id == req_id
                    and p.generator_id == generator_id
                    and p.status == PLAN_STATUS_FAILED_DETERMINISTIC
                    for p in self.search_plans.values()
                ),
                has_failed_transient=any(
                    p.request_id == req_id
                    and p.generator_id == generator_id
                    and p.status == PLAN_STATUS_FAILED_TRANSIENT
                    for p in self.search_plans.values()
                ),
            )
            counts[bucket] += 1
        return {
            "generator_id": generator_id,
            "wanted_total": wanted_total,
            **counts,
        }

    def log_search(self, request_id: int, query: str | None = None,
                   result_count: int | None = None,
                   elapsed_s: float | None = None,
                   outcome: str = "error",
                   candidates: list[CandidateScore] | None = None,
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
        """Mirror PipelineDB.log_search wire boundary.

        ``candidates`` is encoded via ``msgspec.json.encode`` (same as the
        real DB writer) and stored as a JSON string so tests can decode it
        with ``msgspec.convert(json.loads(row.candidates), type=list[CandidateScore])``
        — the same path U7 will use to read the JSONB blob back.

        U11 forensics kwargs (R22-R27) mirror the production signature.
        Each defaults to ``None`` so legacy ``log_search`` calls in
        tests stay backwards-compatible.
        """
        self._next_search_log_id += 1
        candidates_json: str | None = None
        if candidates is not None:
            import msgspec
            candidates_json = msgspec.json.encode(candidates).decode()
        self.search_logs.append(SearchLogRow(
            request_id=request_id,
            query=query,
            result_count=result_count,
            elapsed_s=elapsed_s,
            outcome=outcome,
            id=self._next_search_log_id,
            candidates=candidates_json,
            variant=variant,
            final_state=final_state,
            browse_time_s=browse_time_s,
            match_time_s=match_time_s,
            peers_browsed=peers_browsed,
            peers_browsed_lazy=peers_browsed_lazy,
            fanout_waves=fanout_waves,
            pre_filter_skip_count=pre_filter_skip_count,
            rejection_reason=rejection_reason,
            result_count_uncapped=result_count_uncapped,
            query_token_count=query_token_count,
            query_distinct_token_count=query_distinct_token_count,
            expected_track_count=expected_track_count,
            matcher_score_top1=matcher_score_top1,
            query_template=query_template,
        ))

    def get_search_history(self,
                           request_id: int) -> list[dict[str, object]]:
        return [
            self._search_log_to_dict(e)
            for e in reversed(self.search_logs)
            if e.request_id == request_id
        ]

    def get_search_plan_stats_history(
        self, request_id: int,
    ) -> list[dict[str, object]]:
        rows = self.get_search_history(request_id)
        return [
            {k: row[k] for k in self._SEARCH_PLAN_STATS_HISTORY_KEYS}
            for row in rows
        ]

    def get_search_history_page(
        self,
        request_id: int,
        *,
        limit: int,
        before_id: int | None = None,
    ) -> SearchLogHistoryPage:
        """Mirror of ``PipelineDB.get_search_history_page``.

        Returns at most ``limit`` rows ``id DESC``; sets
        ``next_before_id`` to the trimmed +1 row's id when a next page
        exists. Same ``id <= before_id`` resume semantics as the real DB
        so the cursor never loses a row at page boundaries.
        """
        from lib.pipeline_db import SearchLogHistoryPage as _Page
        # Walk newest-first; respect ``id <= before_id`` so the cursor
        # round-trip resumes exactly at the trimmed row.
        rows: list[dict[str, object]] = []
        for entry in reversed(self.search_logs):
            if entry.request_id != request_id:
                continue
            if before_id is not None and entry.id > before_id:
                continue
            rows.append(self._search_log_to_dict(entry))
            if len(rows) >= int(limit) + 1:
                break
        next_before_id: int | None = None
        if len(rows) > int(limit):
            extra = rows.pop()
            extra_id = extra["id"]
            assert isinstance(extra_id, int)
            next_before_id = extra_id
        return _Page(rows=rows, next_before_id=next_before_id)

    def get_saturation_summary(
        self, request_id: int, *, window_days: int = 14,
    ) -> SaturationSummary:
        """U7 mirror of ``PipelineDB.get_saturation_summary``.

        Replicates the SQL aggregate against ``self.search_logs``:
        rows whose ``final_state`` contains ``LimitReached`` count as
        saturated; ``pre_filter_skip_count`` is summed. The window cut
        uses Python ``datetime`` arithmetic so tests can rewind
        ``SearchLogRow.created_at`` deterministically.

        ``saturation_rate`` is ``0.0`` (not NaN) when the window
        contains no rows — the assembly (rate + zero-total fallback) is
        production's own ``saturation_summary_from_counts``; only the
        window cut is adapter-local.
        """
        cutoff = _utcnow() - timedelta(days=int(window_days))
        total = 0
        saturated = 0
        skips = 0
        for entry in self.search_logs:
            if entry.request_id != request_id:
                continue
            if entry.created_at <= cutoff:
                continue
            total += 1
            if entry.final_state is not None and "LimitReached" in entry.final_state:
                saturated += 1
            skips += int(entry.pre_filter_skip_count or 0)
        return saturation_summary_from_counts(
            total_searches=total,
            saturated_searches=saturated,
            total_pre_filter_skips=skips,
            window_days=int(window_days),
        )

    def get_legacy_search_log_summary(
        self, request_id: int, *, limit: int,
    ) -> tuple[int, list[dict[str, object]]]:
        legacy = [
            self._search_log_to_dict(e)
            for e in reversed(self.search_logs)
            if e.request_id == request_id and e.plan_id is None
        ]
        head = [
            {
                "id": row.get("id"),
                "request_id": row.get("request_id"),
                "query": row.get("query"),
                "result_count": row.get("result_count"),
                "elapsed_s": row.get("elapsed_s"),
                "outcome": row.get("outcome"),
                "variant": row.get("variant"),
                "final_state": row.get("final_state"),
                "created_at": row.get("created_at"),
            }
            for row in legacy[:limit]
        ]
        return len(legacy), head

    def create_successful_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
        set_active: bool = True,
    ) -> int:
        if not items:
            raise ValueError(
                "create_successful_search_plan requires at least one item; "
                "use create_failed_search_plan for empty results.")
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        if self._requests[request_id].get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        # Mirror the partial unique index "one active plan per request".
        if set_active:
            for existing in self.search_plans.values():
                if (existing.request_id == request_id
                        and existing.status == PLAN_STATUS_ACTIVE):
                    raise ValueError(
                        f"request {request_id} already has an active plan; "
                        "use supersede_search_plan_with_replacement to replace it")
        # Snapshot per-item ordinals are unique by definition of the input
        # ordering; mirror the (plan, ordinal) UNIQUE constraint.
        seen_ords: set[int] = set()
        for it in items:
            if it.ordinal in seen_ords:
                raise ValueError(
                    f"duplicate plan ordinal {it.ordinal}")
            if not it.query.strip():
                raise ValueError("plan items require non-empty queries")
            seen_ords.add(it.ordinal)

        self._next_search_plan_id += 1
        plan_id = self._next_search_plan_id
        self.search_plans[plan_id] = _FakeSearchPlanRow(
            id=plan_id,
            request_id=request_id,
            generator_id=generator_id,
            status=PLAN_STATUS_ACTIVE,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        for it in items:
            self._next_search_plan_item_id += 1
            self.search_plan_items[self._next_search_plan_item_id] = (
                _FakeSearchPlanItemRow(
                    id=self._next_search_plan_item_id,
                    plan_id=plan_id,
                    ordinal=it.ordinal,
                    strategy=it.strategy,
                    query=it.query,
                    canonical_query_key=it.canonical_query_key,
                    repeat_group=it.repeat_group,
                    provenance=copy.deepcopy(it.provenance)
                        if it.provenance is not None else None,
                )
            )
        if set_active:
            row = self._requests[request_id]
            row["active_plan_id"] = plan_id
            row["next_plan_ordinal"] = 0
            row["plan_cycle_count"] = 0
            row["updated_at"] = _utcnow()
        return plan_id

    def create_failed_search_plan(
        self,
        *,
        request_id: int,
        generator_id: str,
        failure_class: str,
        error_message: str | None = None,
        transient: bool,
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> int:
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        if self._requests[request_id].get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        status = (
            PLAN_STATUS_FAILED_TRANSIENT if transient
            else PLAN_STATUS_FAILED_DETERMINISTIC
        )
        self._next_search_plan_id += 1
        plan_id = self._next_search_plan_id
        self.search_plans[plan_id] = _FakeSearchPlanRow(
            id=plan_id,
            request_id=request_id,
            generator_id=generator_id,
            status=status,
            failure_class=failure_class,
            error_message=error_message,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        return plan_id

    def supersede_search_plan_with_replacement(
        self,
        *,
        request_id: int,
        generator_id: str,
        items: list[SearchPlanItemInput],
        metadata_snapshot: dict[str, Any] | None = None,
        provenance: dict[str, Any] | None = None,
    ) -> int:
        if not items:
            raise ValueError(
                "supersede_search_plan_with_replacement requires items.")
        if request_id not in self._requests:
            raise ValueError(f"request {request_id} not found")
        row = self._requests[request_id]
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        old_id = row.get("active_plan_id")
        now = _utcnow()
        if old_id is not None:
            old = self.search_plans.get(old_id)
            if old is not None:
                old.status = PLAN_STATUS_SUPERSEDED
                old.superseded_at = now

        # Bypass the "no active plan" guard since we just demoted the old one.
        self._next_search_plan_id += 1
        new_id = self._next_search_plan_id
        self.search_plans[new_id] = _FakeSearchPlanRow(
            id=new_id,
            request_id=request_id,
            generator_id=generator_id,
            status=PLAN_STATUS_ACTIVE,
            metadata_snapshot=copy.deepcopy(metadata_snapshot)
                if metadata_snapshot is not None else None,
            provenance=copy.deepcopy(provenance)
                if provenance is not None else None,
        )
        for it in items:
            self._next_search_plan_item_id += 1
            self.search_plan_items[self._next_search_plan_item_id] = (
                _FakeSearchPlanItemRow(
                    id=self._next_search_plan_item_id,
                    plan_id=new_id,
                    ordinal=it.ordinal,
                    strategy=it.strategy,
                    query=it.query,
                    canonical_query_key=it.canonical_query_key,
                    repeat_group=it.repeat_group,
                    provenance=copy.deepcopy(it.provenance)
                        if it.provenance is not None else None,
                )
            )
        if old_id is not None:
            old = self.search_plans.get(old_id)
            if old is not None:
                old.superseded_by_plan_id = new_id
        row["active_plan_id"] = new_id
        row["next_plan_ordinal"] = 0
        row["plan_cycle_count"] = 0
        row["updated_at"] = now
        return new_id

    def _items_for_plan(self, plan_id: int) -> list[SearchPlanItemRow]:
        rows = [
            it for it in self.search_plan_items.values()
            if it.plan_id == plan_id
        ]
        rows.sort(key=lambda r: r.ordinal)
        return [
            SearchPlanItemRow(
                id=r.id,
                plan_id=r.plan_id,
                ordinal=r.ordinal,
                strategy=r.strategy,
                query=r.query,
                canonical_query_key=r.canonical_query_key,
                repeat_group=r.repeat_group,
                provenance=(
                    SearchPlanItemProvenance(
                        values=msgspec.convert(
                            copy.deepcopy(r.provenance),
                            type=dict[str, Any],
                        )
                    )
                    if r.provenance is not None else None
                ),
            )
            for r in rows
        ]

    def _plan_to_row(self, plan: _FakeSearchPlanRow) -> SearchPlanRow:
        return SearchPlanRow(
            id=plan.id,
            request_id=plan.request_id,
            generator_id=plan.generator_id,
            status=plan.status,
            failure_class=plan.failure_class,
            metadata_snapshot=(
                msgspec.convert(
                    copy.deepcopy(plan.metadata_snapshot),
                    type=SearchPlanMetadataSnapshot,
                )
                if plan.metadata_snapshot is not None else None
            ),
            provenance=(
                SearchPlanProvenance(
                    values=msgspec.convert(
                        copy.deepcopy(plan.provenance),
                        type=dict[str, Any],
                    )
                )
                if plan.provenance is not None else None
            ),
            error_message=plan.error_message,
            superseded_at=plan.superseded_at,
            superseded_by_plan_id=plan.superseded_by_plan_id,
            created_at=plan.created_at,
        )

    def get_active_search_plan(
        self,
        request_id: int,
    ) -> ActiveSearchPlan | None:
        row = self._requests.get(request_id)
        if row is None:
            return None
        plan_id = row.get("active_plan_id")
        if plan_id is None:
            return None
        plan = self.search_plans.get(plan_id)
        if plan is None:
            return None
        return ActiveSearchPlan(
            plan=self._plan_to_row(plan),
            items=self._items_for_plan(plan_id),
            next_ordinal=int(row.get("next_plan_ordinal") or 0),
            cycle_count=int(row.get("plan_cycle_count") or 0),
        )

    def advance_search_plan_cursor(
        self,
        request_id: int,
        *,
        target_ordinal: int,
        plan_item_count: int,
    ) -> tuple[int, int, int]:
        """Mirror of ``PipelineDB.advance_search_plan_cursor``.

        Forward-only operator-driven cursor advance. Validates inputs the
        same way the real method does, raising ``ValueError`` for missing
        request, no active plan, out-of-range target, or backward intent.
        """
        if plan_item_count <= 0:
            raise ValueError(
                f"plan_item_count must be > 0 (got {plan_item_count})")
        if target_ordinal < 0 or target_ordinal >= plan_item_count:
            raise ValueError(
                f"target_ordinal {target_ordinal} out of range "
                f"[0, {plan_item_count})")
        row = self._requests.get(request_id)
        if row is None:
            raise ValueError(f"request {request_id} not found")
        if row.get("status") == "replaced":
            raise ReplacedRequestMutationError(request_id)
        active_plan_id = row.get("active_plan_id")
        if active_plan_id is None:
            raise ValueError(
                f"request {request_id} has no active plan")
        previous_ordinal = int(row.get("next_plan_ordinal") or 0)
        if target_ordinal <= previous_ordinal:
            raise ValueError(
                f"target_ordinal {target_ordinal} must be greater than "
                f"current next_plan_ordinal {previous_ordinal} "
                "(advance is forward-only; use regenerate for backward "
                "intent)")
        row["next_plan_ordinal"] = target_ordinal
        # Cursor-mutation recorder for the U13 R20 runtime guard.
        self.advance_search_plan_cursor_calls.append(
            (request_id, previous_ordinal, int(target_ordinal)),
        )
        return (int(active_plan_id), previous_ordinal, target_ordinal)

    def list_wanted_for_plan_reconciliation(
        self,
    ) -> list[WantedReconciliationCandidate]:
        out: list[WantedReconciliationCandidate] = []
        for rid in sorted(self._requests.keys()):
            r = self._requests[rid]
            if r.get("status") != "wanted":
                continue
            plan_id = r.get("active_plan_id")
            gen_id: str | None = None
            if plan_id is not None:
                plan = self.search_plans.get(plan_id)
                if plan is not None and plan.status == PLAN_STATUS_ACTIVE:
                    gen_id = plan.generator_id
                else:
                    plan_id = None
            out.append(WantedReconciliationCandidate(
                request_id=rid,
                active_plan_id=plan_id,
                active_plan_generator_id=gen_id,
                next_plan_ordinal=int(r.get("next_plan_ordinal") or 0),
                plan_cycle_count=int(r.get("plan_cycle_count") or 0),
            ))
        return out

    def list_search_plan_classification_for_requests(
        self,
        request_ids: list[int],
    ) -> dict[int, DryRunPlanClassification]:
        """Mirror of ``PipelineDB.list_search_plan_classification_for_requests``.

        Walks ``self.search_plans`` once and returns the latest failed
        deterministic / transient generator id per request. Empty input
        returns ``{}`` without scanning.
        """
        if not request_ids:
            return {}
        # Initialise so requests with no failed plan rows still surface
        # in the result with None/None generator ids.
        out: dict[int, DryRunPlanClassification] = {
            int(rid): DryRunPlanClassification(
                request_id=int(rid),
                latest_failed_deterministic_generator_id=None,
                latest_failed_transient_generator_id=None,
                latest_failed_transient_created_at=None,
            )
            for rid in request_ids
        }
        for rid in out:
            det_matches = [
                p for p in self.search_plans.values()
                if p.request_id == rid
                and p.status == PLAN_STATUS_FAILED_DETERMINISTIC
            ]
            trans_matches = [
                p for p in self.search_plans.values()
                if p.request_id == rid
                and p.status == PLAN_STATUS_FAILED_TRANSIENT
            ]
            det_matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            trans_matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            out[rid] = DryRunPlanClassification(
                request_id=rid,
                latest_failed_deterministic_generator_id=(
                    det_matches[0].generator_id if det_matches else None),
                latest_failed_transient_generator_id=(
                    trans_matches[0].generator_id if trans_matches else None),
                latest_failed_transient_created_at=(
                    trans_matches[0].created_at if trans_matches else None),
            )
        return out

    def get_wanted_searchable(
        self,
        generator_id: str,
        limit: int | None = None,
        *,
        title_blacklist: Sequence[str] = (),
        now: datetime | None = None,
    ) -> list[dict[str, Any]]:
        """Mirror of ``PipelineDB.get_wanted_searchable``.

        Returns wanted rows that are due (same backoff gate as
        ``get_wanted``) AND have an active plan whose generator id
        matches ``generator_id``. Rows without a current-generator
        active plan are filtered out.
        """
        snapshot_at = now or _utcnow()
        blacklist = tuple(term.lower() for term in title_blacklist if term)
        eligible: list[dict[str, Any]] = []
        for r in self._requests.values():
            if r.get("status") != "wanted":
                continue
            if (
                r.get("next_retry_after") is not None
                and r["next_retry_after"] > snapshot_at
            ):
                continue
            plan_id = r.get("active_plan_id")
            if plan_id is None:
                continue
            plan = self.search_plans.get(plan_id)
            if plan is None:
                continue
            if plan.status != "active":
                continue
            if plan.generator_id != generator_id:
                continue
            if any(
                entry.source == "youtube"
                and entry.outcome == "youtube_running"
                and entry.request_id == r.get("id")
                for entry in self.download_logs
            ):
                continue
            if any(
                row.get("job_type") == IMPORT_JOB_YOUTUBE
                and row.get("request_id") == r.get("id")
                and row.get("status") in IMPORT_JOB_ACTIVE_STATUSES
                for row in self._import_jobs
            ):
                continue
            title = str(r.get("album_title") or "").lower()
            if any(term in title for term in blacklist):
                continue
            eligible.append(r)
        if limit is None:
            return [copy.deepcopy(r) for r in eligible]
        page_size = int(limit)
        slots = search_cohort_slots(page_size)
        cutoff = snapshot_at - timedelta(hours=NEW_REQUEST_PRIORITY_HOURS)
        new = [
            row for row in eligible
            if (
                self._as_utc(_as_datetime(row.get("created_at"))) > cutoff
                or (
                    row.get("priority_started_at") is not None
                    and self._as_utc(_as_datetime(
                        row.get("priority_started_at"))) > cutoff
                )
            )
        ]
        new_ids = {int(row["id"]) for row in new}
        established = [
            row for row in eligible
            if int(row["id"]) not in new_ids
        ]
        selected = new[:slots.new] + established[:slots.established]
        selected_ids = {int(row["id"]) for row in selected}
        remaining = [
            row for row in eligible
            if int(row["id"]) not in selected_ids
        ]
        selected.extend(remaining[:max(page_size - len(selected), 0)])
        return [copy.deepcopy(r) for r in selected]

    def get_search_plan_inspection(
        self,
        request_id: int,
    ) -> SearchPlanInspection:
        active = self.get_active_search_plan(request_id)

        def _latest(status: str) -> SearchPlanRow | None:
            matches = [
                p for p in self.search_plans.values()
                if p.request_id == request_id and p.status == status
            ]
            if not matches:
                return None
            matches.sort(key=lambda p: (p.created_at, p.id), reverse=True)
            return self._plan_to_row(matches[0])

        superseded = sum(
            1 for p in self.search_plans.values()
            if p.request_id == request_id
            and p.status == PLAN_STATUS_SUPERSEDED
        )
        legacy = sum(
            1 for r in self.search_logs
            if r.request_id == request_id and r.plan_id is None
        )
        return SearchPlanInspection(
            request_id=request_id,
            active=active,
            latest_failed_deterministic=_latest(
                PLAN_STATUS_FAILED_DETERMINISTIC),
            latest_failed_transient=_latest(PLAN_STATUS_FAILED_TRANSIENT),
            superseded_count=superseded,
            legacy_search_log_count=legacy,
        )

    def get_search_plan_stats(
        self,
        request_id: int,
        *,
        current_only: bool = True,
        prefetched_history: list[dict[str, Any]] | None = None,
    ):
        """Mirror of ``PipelineDB.get_search_plan_stats``.

        Re-uses the production aggregation helper so the fake stays in
        lock-step with PostgreSQL behavior — the only thing that
        differs is where the rows come from.
        """
        from lib.pipeline_db import SearchPlanStats, _build_stats_bucket
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
            other_rows: list[dict[str, Any]] = []
            other_legacy: list[dict[str, Any]] = []
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
        # Cursor-mutation recorder for the U13 R20 runtime guard.
        self.record_consumed_search_attempt_calls.append(attempt)
        row = self._requests.get(attempt.request_id)
        if row is None:
            raise ValueError(f"request {attempt.request_id} not found")

        active_plan_id = row.get("active_plan_id")
        next_ordinal = int(row.get("next_plan_ordinal") or 0)
        cycle_count = int(row.get("plan_cycle_count") or 0)
        plan = self.search_plans.get(attempt.plan_id)
        item = self.search_plan_items.get(attempt.plan_item_id)
        if (
            plan is None
            or plan.request_id != attempt.request_id
            or item is None
            or item.plan_id != attempt.plan_id
        ):
            raise ValueError(
                f"plan_item_id={attempt.plan_item_id} does not belong to "
                f"plan_id={attempt.plan_id} for request_id={attempt.request_id}")
        # One shared decision with production's own cursor arm
        # (``lib/pipeline_db/decisions.py``); only the reads around it
        # differ (this dict walk vs a ``FOR UPDATE`` re-read).
        decision = cursor_advance_decision(
            current_status=str(row.get("status") or ""),
            active_plan_id=active_plan_id,
            next_ordinal=next_ordinal,
            cycle_count=cycle_count,
            attempt_plan_id=attempt.plan_id,
            attempt_plan_ordinal=attempt.plan_ordinal,
            attempt_cycle_count_snapshot=attempt.cycle_count_snapshot,
            attempt_plan_item_count=attempt.plan_item_count,
        )
        is_stale = decision.is_stale

        # Snapshot pre-write so a partial mutation can be unwound on
        # validation failure, mirroring the real DB transaction.
        snapshot_request = copy.deepcopy(row)
        snapshot_log_count = len(self.search_logs)
        snapshot_next_id = self._next_search_log_id

        try:
            cursor_update_status = decision.cursor_update_status
            execution_stage = decision.execution_stage
            stale_reason = decision.stale_reason
            new_next_ordinal = decision.new_next_ordinal
            new_cycle = decision.new_cycle_count

            self._next_search_log_id += 1
            log_id = self._next_search_log_id
            self.search_logs.append(SearchLogRow(
                request_id=attempt.request_id,
                query=attempt.query,
                result_count=attempt.result_count,
                elapsed_s=attempt.elapsed_s,
                outcome=attempt.outcome,
                id=log_id,
                candidates=attempt.candidates_json,
                variant=attempt.variant,
                final_state=attempt.final_state,
                browse_time_s=attempt.browse_time_s,
                match_time_s=attempt.match_time_s,
                peers_browsed=attempt.peers_browsed,
                peers_browsed_lazy=attempt.peers_browsed_lazy,
                fanout_waves=attempt.fanout_waves,
                plan_id=attempt.plan_id,
                plan_item_id=attempt.plan_item_id,
                plan_ordinal=attempt.plan_ordinal,
                plan_strategy=attempt.plan_strategy,
                plan_canonical_query_key=attempt.plan_canonical_query_key,
                plan_repeat_group=attempt.plan_repeat_group,
                plan_generator_id=attempt.plan_generator_id,
                execution_stage=execution_stage,
                attempt_consumed=not is_stale,
                cursor_update_status=cursor_update_status,
                stale_reason=stale_reason,
                plan_cycle_snapshot=attempt.cycle_count_snapshot,
                pre_filter_skip_count=attempt.pre_filter_skip_count,
                rejection_reason=attempt.rejection_reason,
                result_count_uncapped=attempt.result_count_uncapped,
                query_token_count=attempt.query_token_count,
                query_distinct_token_count=attempt.query_distinct_token_count,
                expected_track_count=attempt.expected_track_count,
                matcher_score_top1=attempt.matcher_score_top1,
                query_template=attempt.query_template,
                cross_request_conflict_request_ids=(
                    list(attempt.cross_request_conflict_request_ids)
                    if attempt.cross_request_conflict_request_ids
                    else None
                ),
            ))

            now = _utcnow()
            if not is_stale:
                row["next_plan_ordinal"] = new_next_ordinal
                row["plan_cycle_count"] = new_cycle
                row["updated_at"] = now
                if (
                    attempt.apply_scheduler_attempt
                    and not attempt.scheduler_success
                ):
                    new_count = (row.get("search_attempts") or 0) + 1
                    row["search_attempts"] = new_count
                    row["last_attempt_at"] = now
                    backoff_minutes = search_backoff_minutes(new_count - 1)
                    row["next_retry_after"] = (
                        now + timedelta(minutes=backoff_minutes))
                elif (
                    attempt.apply_scheduler_attempt
                    and attempt.scheduler_success
                ):
                    row["last_attempt_at"] = now

                # U12: mirror the wrap-time failure_class write in
                # ``PipelineDB.record_consumed_search_attempt``. The
                # classification runs only on wrap (the cycle that
                # just completed is ``cycle_count``, pre-increment)
                # and only overwrites ``failure_class`` when the
                # classifier returns a non-None verdict — degenerate
                # cycles (zero consumed attempts) preserve the prior
                # value.
                if cursor_update_status == CURSOR_UPDATE_WRAPPED:
                    summaries = [
                        _SearchSummary(
                            outcome=str(lr.outcome),
                            rejection_reason=lr.rejection_reason,
                        )
                        for lr in self.search_logs
                        if (
                            lr.request_id == attempt.request_id
                            and lr.plan_cycle_snapshot == cycle_count
                            and bool(lr.attempt_consumed)
                        )
                    ]
                    verdict = _classify_failure_class(
                        summaries,
                        current_status=str(row.get("status") or "wanted"),
                    )
                    if verdict is not None:
                        row["failure_class"] = verdict
                        row["updated_at"] = now
            return ConsumedAttemptResult(
                search_log_id=log_id,
                cursor_update_status=cursor_update_status,
                new_next_ordinal=new_next_ordinal,
                new_cycle_count=new_cycle,
                is_stale=is_stale,
            )
        except Exception:
            # Roll back the partial mutation so test assertions can prove
            # "log-without-cursor or cursor-without-log" never happens.
            self._requests[attempt.request_id] = snapshot_request
            self.search_logs = self.search_logs[:snapshot_log_count]
            self._next_search_log_id = snapshot_next_id
            raise

    def record_non_consuming_search_attempt(
        self,
        attempt: NonConsumingAttemptInput,
    ) -> int:
        # Cursor-adjacent recorder for the U13 R20 runtime guard. This
        # method does not advance the cursor itself, but it does write
        # ``search_log`` with plan context — the detection job must not
        # call it either (the probe is its own slskd surface and never
        # touches ``search_log``).
        self.record_non_consuming_search_attempt_calls.append(attempt)
        row = self._requests.get(attempt.request_id)
        if row is None:
            raise ValueError(f"request {attempt.request_id} not found")
        cycle_snapshot = int(row.get("plan_cycle_count") or 0)
        self._next_search_log_id += 1
        log_id = self._next_search_log_id
        self.search_logs.append(SearchLogRow(
            request_id=attempt.request_id,
            query=attempt.query,
            result_count=attempt.result_count,
            elapsed_s=attempt.elapsed_s,
            outcome=attempt.outcome,
            id=log_id,
            final_state=attempt.final_state,
            plan_id=attempt.plan_id,
            plan_item_id=attempt.plan_item_id,
            plan_ordinal=attempt.plan_ordinal,
            plan_strategy=attempt.plan_strategy,
            plan_canonical_query_key=attempt.plan_canonical_query_key,
            plan_repeat_group=attempt.plan_repeat_group,
            plan_generator_id=attempt.plan_generator_id,
            execution_stage=SEARCH_LOG_STAGE_PRE_ATTEMPT,
            attempt_consumed=False,
            cursor_update_status=CURSOR_UPDATE_UNCHANGED,
            plan_cycle_snapshot=cycle_snapshot,
            pre_filter_skip_count=attempt.pre_filter_skip_count,
            rejection_reason=attempt.rejection_reason,
            result_count_uncapped=attempt.result_count_uncapped,
            query_token_count=attempt.query_token_count,
            query_distinct_token_count=attempt.query_distinct_token_count,
            expected_track_count=attempt.expected_track_count,
            matcher_score_top1=attempt.matcher_score_top1,
            query_template=attempt.query_template,
        ))
        if attempt.apply_scheduler_attempt and row.get("status") != "replaced":
            now = _utcnow()
            new_count = (row.get("search_attempts") or 0) + 1
            row["search_attempts"] = new_count
            row["last_attempt_at"] = now
            backoff_minutes = search_backoff_minutes(new_count - 1)
            row["next_retry_after"] = now + timedelta(
                minutes=backoff_minutes)
            row["updated_at"] = now
        return log_id

