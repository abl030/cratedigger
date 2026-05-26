"""Startup search-plan reconciliation.

Runs once per Cratedigger cycle, BEFORE Phase 2 search execution. Walks
every wanted request (ignoring ``next_retry_after`` and the page-size
limit ``get_wanted`` applies) and ensures each row has either:

  * an active successful plan whose generator_id matches the current
    ``SEARCH_PLAN_GENERATOR_ID``, OR
  * a visible failed/retryable plan state that explains why no current
    plan exists.

Any other state (no plan + no failure record on the current generator
id) is a stop-the-deploy signal — logged at ERROR with the request id so
ops can investigate before the next cycle.

The reconciliation step is intentionally:

  * **Per-row isolated.** One request's exception cannot block the
    others; we catch and log, then continue.
  * **Idempotent + resumable.** ``SearchPlanService.generate_for_request``
    no-ops when a current active plan already exists and supersedes
    old-generator plans. Re-running the loop does not duplicate plans
    or lose failure state.
  * **Non-blocking on transient failures.** A resolver outage records a
    transient failed plan and surfaces in the readiness counts; the
    cycle continues.
  * **Dry-run capable.** ``dry_run=True`` runs the all-wanted scan and
    classifies each row WITHOUT calling the service — useful for deploy
    verification.

Returns one ``ReconciliationSummary`` per call. Counts must reconcile
exactly to ``wanted_total`` (sum of all classified buckets).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from lib.search import SEARCH_PLAN_GENERATOR_ID
from lib.search_plan_service import (
    RESULT_FAILED_DETERMINISTIC,
    RESULT_FAILED_TRANSIENT,
    RESULT_NOOP_ACTIVE_PLAN_EXISTS,
    RESULT_REQUEST_NOT_FOUND,
    RESULT_SUCCESS,
    SearchPlanService,
    ServiceResult,
    _within_retry_window,
)

if TYPE_CHECKING:
    from lib.pipeline_db import (
        DryRunPlanClassification,
        PipelineDB,
        SearchPlanInspection,
        WantedReconciliationCandidate,
    )

logger = logging.getLogger(__name__)


# Default progress-log batch size. Production has ~600 wanted rows; one
# log line per 100 rows keeps progress visible without flooding journals.
DEFAULT_PROGRESS_BATCH_SIZE = 100


@dataclass(frozen=True)
class ReconciliationSummary:
    """Outcome counters for one reconciliation pass.

    Sums of the classified buckets (``active_current``, ``generated``,
    ``old_generator_replaced``, ``deterministic_failed``,
    ``retryable_failed``, ``skipped``, ``unclassified_no_plan``) MUST
    equal ``wanted_total``. ``unclassified_no_plan > 0`` is the
    stop-the-deploy signal: a wanted row exists with no plan AND no
    explanatory failure record on the current generator id.
    """

    generator_id: str
    wanted_total: int
    active_current: int
    generated: int
    old_generator_replaced: int
    deterministic_failed: int
    retryable_failed: int
    skipped: int
    unclassified_no_plan: int
    duration_s: float
    dry_run: bool

    def to_log_line(self) -> str:
        """Render the canonical one-line summary for cycle logs."""
        return (
            "search_plan_reconciliation "
            f"generator_id={self.generator_id} "
            f"wanted_total={self.wanted_total} "
            f"active_current={self.active_current} "
            f"generated={self.generated} "
            f"old_generator_replaced={self.old_generator_replaced} "
            f"deterministic_failed={self.deterministic_failed} "
            f"retryable_failed={self.retryable_failed} "
            f"skipped={self.skipped} "
            f"unclassified_no_plan={self.unclassified_no_plan} "
            f"duration_s={self.duration_s:.2f} "
            f"dry_run={'true' if self.dry_run else 'false'}"
        )

    @property
    def total_classified(self) -> int:
        return (
            self.active_current
            + self.generated
            + self.old_generator_replaced
            + self.deterministic_failed
            + self.retryable_failed
            + self.skipped
            + self.unclassified_no_plan
        )

    @property
    def is_ready(self) -> bool:
        """True iff every wanted row classified into an explainable bucket."""
        return self.unclassified_no_plan == 0 and self.total_classified == self.wanted_total


class _DBProto(Protocol):
    """Minimal DB surface ``reconcile_search_plans`` needs.

    Both ``PipelineDB`` and ``FakePipelineDB`` satisfy this; we keep
    typing structural so tests can pass either.
    """

    def list_wanted_for_plan_reconciliation(
        self,
    ) -> "list[WantedReconciliationCandidate]": ...

    def get_search_plan_inspection(
        self,
        request_id: int,
    ) -> "SearchPlanInspection": ...

    def list_search_plan_classification_for_requests(
        self,
        request_ids: list[int],
    ) -> "dict[int, DryRunPlanClassification]": ...


def reconcile_search_plans(
    db: "PipelineDB | _DBProto",
    service: SearchPlanService | None,
    *,
    dry_run: bool = False,
    generator_id: str | None = None,
    progress_batch_size: int = DEFAULT_PROGRESS_BATCH_SIZE,
) -> ReconciliationSummary:
    """Walk every wanted request and ensure plan readiness.

    Args:
        db: PipelineDB (or compatible fake) for the all-wanted query
            and inspection lookups.
        service: SearchPlanService used to generate / supersede plans.
            May be ``None`` only when ``dry_run=True`` -- the dry-run
            path classifies existing state without producing plans.
        dry_run: When True, classify each wanted row but do NOT call
            the service. The returned summary buckets reflect what
            *would* be done.
        generator_id: Override the current generator id. Defaults to
            ``service.generator_id`` (live runs) or
            ``SEARCH_PLAN_GENERATOR_ID`` (dry runs). The override is a
            test seam; production must NOT pass this.
        progress_batch_size: Emit one progress-log line every N rows.

    Returns:
        ReconciliationSummary with reconciled counts. The caller decides
        what to do with ``unclassified_no_plan > 0`` (typically: log at
        ERROR and continue -- the cycle should not block on unrelated
        rows).
    """
    if not dry_run and service is None:
        raise ValueError("service is required unless dry_run=True")
    if generator_id is None:
        generator_id = (
            service.generator_id if service is not None
            else SEARCH_PLAN_GENERATOR_ID
        )
    elif service is not None and generator_id != service.generator_id:
        # Refuse to silently classify against a different id than the
        # service will generate against -- that would produce
        # unactionable summaries.
        raise ValueError(
            f"generator_id={generator_id!r} differs from "
            f"service.generator_id={service.generator_id!r}; do not "
            f"override unless dry_run=True")

    started = time.time()
    candidates = list(db.list_wanted_for_plan_reconciliation())
    wanted_total = len(candidates)

    # Pre-fetch failed-plan classification for rows without an active
    # plan. Dry-run uses it to stay read-only; live reconciliation uses
    # it to avoid re-inspecting sticky deterministic / recent transient
    # failures every cycle.
    plan_classifications: "dict[int, DryRunPlanClassification]" = {}
    if candidates:
        # Only request rows that actually need classification: rows
        # without an active plan. Rows with an active plan are
        # classified from candidate fields or passed through the service
        # for track-count drift checks.
        needing_classification = [
            c.request_id for c in candidates if c.active_plan_id is None
        ]
        if needing_classification:
            plan_classifications = (
                db.list_search_plan_classification_for_requests(
                    needing_classification)
            )

    # Per-bucket request_ids for the operator-facing INFO surfacing
    # after the loop. Counts are still summed for ReconciliationSummary;
    # the id lists exist so an operator can spot-check which requests
    # ended up in each non-trivial bucket without a follow-up SQL query.
    bucket_ids: dict[str, list[int]] = {
        "active_current": [],
        "generated": [],
        "old_generator_replaced": [],
        "deterministic_failed": [],
        "retryable_failed": [],
        "skipped": [],
        "unclassified_no_plan": [],
    }

    for index, candidate in enumerate(candidates, start=1):
        try:
            outcome = _reconcile_one(
                db, service, candidate,
                generator_id=generator_id, dry_run=dry_run,
                plan_classification=plan_classifications.get(
                    candidate.request_id),
            )
        except Exception as exc:  # noqa: BLE001 — per-row isolation
            # Per-row isolation: one row's exception must not stop the
            # others. We surface the request id so the operator can
            # follow up.
            logger.exception(
                "search_plan_reconciliation: request_id=%s raised %s; "
                "treating as unclassified no-plan",
                candidate.request_id, exc,
            )
            outcome = "unclassified_no_plan"

        bucket_ids[outcome].append(candidate.request_id)
        if outcome == "unclassified_no_plan":
            logger.error(
                "search_plan_reconciliation: request_id=%s wanted with no "
                "active plan and no current-generator failure record -- "
                "stop-the-deploy signal",
                candidate.request_id,
            )

        if progress_batch_size > 0 and index % progress_batch_size == 0:
            logger.info(
                "search_plan_reconciliation progress: %d/%d processed "
                "(active=%d generated=%d replaced=%d det_fail=%d "
                "trans_fail=%d skipped=%d unclassified=%d)",
                index, wanted_total,
                len(bucket_ids["active_current"]),
                len(bucket_ids["generated"]),
                len(bucket_ids["old_generator_replaced"]),
                len(bucket_ids["deterministic_failed"]),
                len(bucket_ids["retryable_failed"]),
                len(bucket_ids["skipped"]),
                len(bucket_ids["unclassified_no_plan"]),
            )

    # Per-bucket id surfacing (issue #377). Skip the routine buckets
    # (active_current, generated) — on first-deploy cycles ``generated``
    # alone can be ~600 ids and dwarfs the interesting buckets. The
    # remaining buckets are operator-actionable enough that listing
    # their contents pays for itself.
    for bucket_name in (
        "old_generator_replaced",
        "deterministic_failed",
        "retryable_failed",
        "skipped",
        "unclassified_no_plan",
    ):
        ids = bucket_ids[bucket_name]
        if ids:
            logger.info(
                "search_plan_reconciliation bucket=%s count=%d "
                "request_ids=%s",
                bucket_name, len(ids), ids,
            )

    duration_s = time.time() - started
    summary = ReconciliationSummary(
        generator_id=generator_id,
        wanted_total=wanted_total,
        active_current=len(bucket_ids["active_current"]),
        generated=len(bucket_ids["generated"]),
        old_generator_replaced=len(bucket_ids["old_generator_replaced"]),
        deterministic_failed=len(bucket_ids["deterministic_failed"]),
        retryable_failed=len(bucket_ids["retryable_failed"]),
        skipped=len(bucket_ids["skipped"]),
        unclassified_no_plan=len(bucket_ids["unclassified_no_plan"]),
        duration_s=duration_s,
        dry_run=dry_run,
    )
    return summary


def _reconcile_one(
    db: "PipelineDB | _DBProto",
    service: SearchPlanService | None,
    candidate,  # WantedReconciliationCandidate
    *,
    generator_id: str,
    dry_run: bool,
    plan_classification: "DryRunPlanClassification | None" = None,
) -> str:
    """Classify and (when not dry-run) repair one wanted row.

    Returns one of: ``active_current``, ``generated``,
    ``old_generator_replaced``, ``deterministic_failed``,
    ``retryable_failed``, ``skipped``, ``unclassified_no_plan``.
    """
    request_id = candidate.request_id

    # Dry-run fast path: row already has an active plan on the current
    # generator. Live reconciliation still calls the service so it can
    # repair current plans whose persisted snapshot is now stale, such as
    # a plan generated before all tracks were attached.
    if (candidate.active_plan_id is not None
            and candidate.active_plan_generator_id == generator_id):
        if dry_run:
            return "active_current"

    if dry_run:
        return _classify_dry_run(
            db, candidate, generator_id=generator_id,
            classification=plan_classification,
        )

    assert service is not None  # _ checked at function entry
    had_old_generator_active = (
        candidate.active_plan_id is not None
        and candidate.active_plan_generator_id is not None
        and candidate.active_plan_generator_id != generator_id
    )
    if candidate.active_plan_id is None:
        shortcut = _classify_live_sticky_failure(
            plan_classification, generator_id=generator_id)
        if shortcut is not None:
            return shortcut

    # No current-generator active plan: ask the service to repair.
    # ``regenerate=False`` means: no-op when an active *current* plan
    # already exists; otherwise generate (or supersede an old-generator
    # plan, which the service handles internally).
    result: ServiceResult = service.generate_for_request(
        request_id, regenerate=False,
    )

    if result.outcome == RESULT_SUCCESS:
        if had_old_generator_active:
            return "old_generator_replaced"
        return "generated"
    if result.outcome == RESULT_NOOP_ACTIVE_PLAN_EXISTS:
        # Race: another caller (CLI add, web add, prior cycle) generated
        # between our list scan and the service call. Treat as already
        # current.
        return "active_current"
    if result.outcome == RESULT_FAILED_DETERMINISTIC:
        return "deterministic_failed"
    if result.outcome == RESULT_FAILED_TRANSIENT:
        return "retryable_failed"
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        # The row was deleted between the all-wanted scan and the
        # service call. Skip.
        return "skipped"

    # Unknown outcome -- treat as transient retryable so we surface it
    # without blocking the cycle.
    logger.warning(
        "search_plan_reconciliation: request_id=%s unknown service "
        "outcome %r; counting as retryable_failed",
        request_id, result.outcome,
    )
    return "retryable_failed"


def _classify_live_sticky_failure(
    classification: "DryRunPlanClassification | None",
    *,
    generator_id: str,
) -> str | None:
    if classification is None:
        return None
    if classification.latest_failed_deterministic_generator_id == generator_id:
        return "deterministic_failed"
    if (
        classification.latest_failed_transient_generator_id == generator_id
        and _within_retry_window(
            classification.latest_failed_transient_created_at)
    ):
        return "retryable_failed"
    return None


def _classify_dry_run(
    db: "PipelineDB | _DBProto",
    candidate,  # WantedReconciliationCandidate
    *,
    generator_id: str,
    classification: "DryRunPlanClassification | None" = None,
) -> str:
    """Read-only classification path for dry-run.

    Mirrors the buckets the live reconciliation would assign without
    persisting anything.

    The optional ``classification`` argument carries the latest
    failed-deterministic / failed-transient generator ids for this
    request, pre-fetched in one batch by ``reconcile_search_plans``
    so we do not pay 5 queries per row × N rows. When omitted (single
    callers, tests), we fall back to the per-row inspection call.
    """
    if candidate.active_plan_id is not None:
        if candidate.active_plan_generator_id == generator_id:
            return "active_current"
        # Old-generator active plan: live run would supersede it.
        return "old_generator_replaced"

    # No active plan: distinguish missing vs. failed-recorded.
    if classification is not None:
        det_gen = classification.latest_failed_deterministic_generator_id
        trans_gen = classification.latest_failed_transient_generator_id
    else:
        inspection = db.get_search_plan_inspection(candidate.request_id)
        det = inspection.latest_failed_deterministic
        trans = inspection.latest_failed_transient
        det_gen = det.generator_id if det is not None else None
        trans_gen = trans.generator_id if trans is not None else None

    if det_gen == generator_id:
        return "deterministic_failed"
    if trans_gen == generator_id:
        return "retryable_failed"
    # Neither active nor failed -- live run would generate.
    return "generated"
