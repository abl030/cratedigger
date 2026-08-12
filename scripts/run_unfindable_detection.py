#!/usr/bin/env python3
"""Daily unfindable-detection oneshot (U13 — R18-R20).

Runs as `cratedigger-unfindable.service`, scheduled by the
`cratedigger-unfindable.timer` (`OnCalendar=daily`). Completely
separate from the 5-min `cratedigger.service` loop — see
``lib/unfindable_detection_service.py`` for the architectural
rationale (R20 cadence-never-changes invariant).

Wires together the runtime config, a real PipelineDB, and a real
slskd client, then drives ``UnfindableDetectionService.
categorise_due_batch`` over the K oldest cohort members. Per-row
outcomes are logged as structured INFO lines so operators can grep
``journalctl -u cratedigger-unfindable`` for "categorised" /
"downgraded" / "probe_failed".

Run-health / exit code (issue #1090): a fully classified run returns
``0``. When ``categorise_due_batch``'s circuit breaker trips (a
sustained run of slskd search-submit failures), the process returns
``EXIT_INCOMPLETE_RUN`` so systemd reports the unit as failed --
distinct from the pre-existing config/schema abort (``EXIT_CONFIG_ABORT``)
returned before any work runs. See ``_process_batch``.

Run-metrics telemetry (issue #1112): every batch pass that actually
starts -- a fully classified run AND a breaker-tripped one -- attempts
one ``unfindable_run_metrics`` write via
``PipelineDB.record_unfindable_run_metrics`` so the web dashboard can
show run health without scraping journal logs. A run that aborts before
any probe (``EXIT_CONFIG_ABORT`` -- missing slskd config or a
behind/missing schema) writes NOTHING: there is no cohort/backlog
reading to report, and a behind schema may not even have the table yet.
The write is non-fatal (review round 1 F10): a DB error on the
telemetry insert is logged and swallowed rather than retried or
propagated, never turned into a failed unit for a run that otherwise
classified cleanly -- so one attempted write does not guarantee one
persisted row. Accepted residual: a SIGTERM/OOM-killed run leaves no
row, indistinguishable here from the timer never firing -- the systemd
unit's own failure/inactive state is the operator's signal for that case.

The script is intentionally narrow: it does not import any
cursor-mutating PipelineDB methods, plan-service module, or
search-execution module. The R20 AST guard test enforces that on
every change.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Protocol

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.config import CratediggerConfig, read_runtime_config
from lib.migrator import SchemaBehindError, assert_schema_current
from lib.pipeline_db import DEFAULT_DSN, PipelineDB
from lib.slskd_client import SlskdClient
from lib.unfindable_detection_service import (
    DEFAULT_BATCH_SIZE,
    RESULT_CATEGORISED,
    RESULT_DOWNGRADED,
    RESULT_NO_CHANGE,
    RESULT_NOT_DUE,
    RESULT_PROBE_FAILED,
    RESULT_REQUEST_NOT_FOUND,
    UnfindableBatchResult,
    UnfindableDetectionService,
    UnfindableServiceResult,
)

logger = logging.getLogger("cratedigger-unfindable")

# Missing slskd config or a behind/missing DB schema -- returned directly
# from main() before any probe work runs. Pre-existing behaviour; named
# (issue #1090 NIT-8) so both abort sites share one producer and
# EXIT_INCOMPLETE_RUN's distinctness from it is a constant comparison, not
# a hand-typed literal.
EXIT_CONFIG_ABORT = 2

# Distinct from EXIT_CONFIG_ABORT. Issue #1090: the run attempted work but
# the circuit breaker stopped it early after a sustained slskd submit
# outage -- systemd shows the unit FAILED (surfacing the incomplete run
# distinctly from a fully classified one) while the daily timer still
# fires next cycle and nothing is parked on any request row (invariant
# 11) -- untouched candidates simply roll forward via the normal
# oldest-probe-first ordering.
EXIT_INCOMPLETE_RUN = 3


class _MetricsDBProto(Protocol):
    """Narrow DB surface ``_process_batch`` needs for run telemetry --
    keeps it FakePipelineDB-friendly (same Protocol pattern as
    ``lib.plex_pin_service._PinDBProto``). Deliberately separate from
    ``lib.unfindable_detection_service._PipelineDBProto``: run-level
    telemetry is this script's concern, not the per-request
    categorisation service's."""

    def record_unfindable_run_metrics(
        self,
        *,
        cohort_total: int,
        due_backlog_at_start: int,
        batch_limit: int,
        candidates_processed: int,
        probes_attempted: int,
        breaker_tripped: bool,
        duration_seconds: float,
        categorised_count: int = 0,
        downgraded_count: int = 0,
        no_change_count: int = 0,
        probe_failed_count: int = 0,
        not_due_count: int = 0,
        request_not_found_count: int = 0,
    ) -> int: ...


def _build_slskd_client(cfg: CratediggerConfig) -> SlskdClient:
    """Construct the slskd client from the runtime config.

    Mirrors ``cratedigger._create_slskd_client`` minus the connection-
    pool tuning — the detection job issues at most one search per
    cohort member per day, so the default pool is fine.
    """
    return SlskdClient(
        host=cfg.slskd_host_url,
        api_key=cfg.resolved_slskd_api_key(),
        url_base=cfg.slskd_url_base,
        timeout=30,
    )


def _summarise(results: list[UnfindableServiceResult]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.outcome] = counts.get(r.outcome, 0) + 1
    return counts


def _log_row_outcome(r: UnfindableServiceResult) -> None:
    if r.outcome == RESULT_CATEGORISED:
        logger.info(
            "categorised request=%s prev=%r new=%r "
            "probe_match_count=%s reason=%r",
            r.request_id, r.previous_category,
            r.new_category, r.probe_match_count, r.reason,
        )
    elif r.outcome == RESULT_DOWNGRADED:
        logger.info(
            "downgraded request=%s prev=%r "
            "probe_match_count=%s",
            r.request_id, r.previous_category,
            r.probe_match_count,
        )
    elif r.outcome == RESULT_NO_CHANGE:
        logger.info(
            "no_change request=%s probe_match_count=%s",
            r.request_id, r.probe_match_count,
        )
    elif r.outcome == RESULT_PROBE_FAILED:
        logger.warning(
            "probe_failed request=%s error=%r",
            r.request_id, r.error_message,
        )
    elif r.outcome == RESULT_NOT_DUE:
        # Should never come back from the batch path — the candidate
        # list already filtered by cadence. Log if it does so the
        # operator notices the drift.
        logger.debug(
            "not_due (unexpected from batch) request=%s", r.request_id,
        )
    elif r.outcome == RESULT_REQUEST_NOT_FOUND:
        logger.warning(
            "request_not_found request=%s (race with operator?)",
            r.request_id,
        )


def _process_batch(
    service: UnfindableDetectionService,
    db: _MetricsDBProto,
    *,
    limit: int,
    cohort_total: int,
    due_backlog_at_start: int,
) -> int:
    """Run one due-batch pass, log per-row + summary, record run
    telemetry, return the process exit code (issues #1090, #1112).

    Returns 0 for a fully classified run. Returns ``EXIT_INCOMPLETE_RUN``
    when ``UnfindableBatchResult.breaker_tripped`` is True — the circuit
    breaker stopped the batch early after a sustained run of slskd
    submit failures. Either way, every row actually attempted already
    followed the conservative write rule (a probe-failed outcome writes
    nothing); this function only decides the process exit code, it never
    marks or parks any request.

    Attempts exactly one ``unfindable_run_metrics`` write per call —
    including a breaker-tripped call — via
    ``db.record_unfindable_run_metrics``. A failed/partial run is
    exactly the signal an operator needs to see on the dashboard. The
    write itself is non-fatal to the run (issue #1112 review F10): a DB
    hiccup on the telemetry insert is logged and swallowed rather than
    retried or propagated, so it never turns a run that otherwise
    classified cleanly into a failed unit — but it also means that one
    call does not guarantee one persisted row. Accepted residual: a run
    that is SIGTERM'd/OOM-killed before reaching this point leaves no
    row at all, indistinguishable here from the timer simply never
    firing — the systemd unit's own failure/inactive state is the
    operator's signal for that case, not this table.
    """
    started = time.monotonic()
    batch: UnfindableBatchResult = service.categorise_due_batch(limit=int(limit))
    duration_s = time.monotonic() - started
    counts = _summarise(batch.results)
    for r in batch.results:
        _log_row_outcome(r)
    candidates_processed = len(batch.results)
    probes_attempted = (
        candidates_processed
        - counts.get(RESULT_NOT_DUE, 0)
        - counts.get(RESULT_REQUEST_NOT_FOUND, 0)
    )
    try:
        db.record_unfindable_run_metrics(
            cohort_total=cohort_total,
            due_backlog_at_start=due_backlog_at_start,
            batch_limit=limit,
            candidates_processed=candidates_processed,
            probes_attempted=probes_attempted,
            breaker_tripped=batch.breaker_tripped,
            duration_seconds=duration_s,
            categorised_count=counts.get(RESULT_CATEGORISED, 0),
            downgraded_count=counts.get(RESULT_DOWNGRADED, 0),
            no_change_count=counts.get(RESULT_NO_CHANGE, 0),
            probe_failed_count=counts.get(RESULT_PROBE_FAILED, 0),
            not_due_count=counts.get(RESULT_NOT_DUE, 0),
            request_not_found_count=counts.get(RESULT_REQUEST_NOT_FOUND, 0),
        )
    except Exception:
        logger.exception(
            "unfindable_detection: failed to record run-metrics telemetry "
            "-- classification results above are unaffected; continuing",
        )
    if batch.breaker_tripped:
        untouched = batch.candidates_considered - len(batch.results)
        logger.error(
            "unfindable_detection: INCOMPLETE run — circuit breaker "
            "tripped after repeated slskd submit failures; attempted=%d/"
            "%d outcomes=%s; %d candidate(s) were never touched this run "
            "and roll into the next daily run",
            len(batch.results), batch.candidates_considered, counts,
            untouched,
        )
        return EXIT_INCOMPLETE_RUN
    logger.info(
        "unfindable_detection: complete; processed=%d/%d outcomes=%s",
        len(batch.results), batch.candidates_considered, counts,
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cratedigger unfindable detection oneshot",
    )
    parser.add_argument(
        "--dsn",
        default=DEFAULT_DSN,
        help="PostgreSQL DSN for the pipeline DB.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=(
            "Maximum cohort members to process this run. "
            "Default is the module-level DEFAULT_BATCH_SIZE."
        ),
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    cfg = read_runtime_config()
    if not cfg.slskd_host_url or not cfg.resolved_slskd_api_key():
        logger.error(
            "unfindable_detection: missing slskd config "
            "(host_url=%r, api_key=<%s>); aborting",
            cfg.slskd_host_url,
            "present" if cfg.resolved_slskd_api_key() else "missing",
        )
        return EXIT_CONFIG_ABORT

    # Fail-loud schema gate. cratedigger-unfindable.service uses Wants=, not
    # Requires=, on cratedigger-db-migrate.service (nix/module.nix) -- see
    # cratedigger.py::main() for the identical gate and its rationale.
    try:
        assert_schema_current(args.dsn)
    except SchemaBehindError as exc:
        logger.error(
            "unfindable_detection: Pipeline DB schema is behind: missing "
            "migration version(s) %s. Refusing to run against an "
            "un-migrated schema -- run cratedigger-db-migrate.service "
            "first.",
            exc.missing_versions,
        )
        return EXIT_CONFIG_ABORT

    db = PipelineDB(args.dsn)
    try:
        slskd_client = _build_slskd_client(cfg)
        service = UnfindableDetectionService(db, slskd_client)
        # Backlog visibility: surface the cohort size before the batch
        # so operators can spot a growing tail. The detection batch is
        # cap-limited at ``--limit`` (default DEFAULT_BATCH_SIZE) per
        # run; if the due-count keeps growing well beyond the cap the
        # daily cadence is no longer draining the backlog and the
        # operator should bump the limit (or the timer frequency).
        # Re-uses the same ``list_unfindable_probe_candidates`` SQL
        # the service uses, just with a generous upper bound so the
        # count reflects the real backlog rather than the batch cap.
        # The probe interval here matches the service's internal
        # constant (no kwarg overrides — single source of truth).
        from lib.unfindable_detection_service import PROBE_INTERVAL_DAYS
        backlog = db.list_unfindable_probe_candidates(
            limit=10_000, probe_interval_days=PROBE_INTERVAL_DAYS,
        )
        # Full cohort size (status='wanted', regardless of probe-due
        # status) -- distinct from ``backlog`` above (due-now only).
        # Reuses the existing ``count_by_status`` aggregate rather than
        # a bespoke COUNT query (.claude/rules/code-quality.md § "No
        # Parallel Code Paths").
        cohort_total = int(db.count_by_status().get("wanted", 0))
        logger.info(
            "unfindable_detection: cohort_total=%d backlog due_count=%d "
            "batch_limit=%d",
            cohort_total, len(backlog), int(args.limit),
        )
        return _process_batch(
            service, db,
            limit=int(args.limit),
            cohort_total=cohort_total,
            due_backlog_at_start=len(backlog),
        )
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
