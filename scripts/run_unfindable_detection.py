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

# Distinct from the pre-existing exit code 2 (missing config / behind-
# schema abort, before any work runs). Issue #1090: the run attempted
# work but the circuit breaker stopped it early after a sustained slskd
# submit outage -- systemd shows the unit FAILED (surfacing the
# incomplete run distinctly from a fully classified one) while the daily
# timer still fires next cycle and nothing is parked on any request row
# (invariant 11) -- untouched candidates simply roll forward via the
# normal oldest-probe-first ordering.
EXIT_INCOMPLETE_RUN = 3


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


def _process_batch(service: UnfindableDetectionService, *, limit: int) -> int:
    """Run one due-batch pass, log per-row + summary, return the process
    exit code (issue #1090).

    Returns 0 for a fully classified run. Returns ``EXIT_INCOMPLETE_RUN``
    when ``UnfindableBatchResult.breaker_tripped`` is True — the circuit
    breaker stopped the batch early after a sustained run of slskd
    submit failures. Either way, every row actually attempted already
    followed the conservative write rule (a probe-failed outcome writes
    nothing); this function only decides the process exit code, it never
    marks or parks any request.
    """
    batch: UnfindableBatchResult = service.categorise_due_batch(limit=int(limit))
    counts = _summarise(batch.results)
    for r in batch.results:
        _log_row_outcome(r)
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
        return 2

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
        return 2

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
        logger.info(
            "unfindable_detection: backlog due_count=%d batch_limit=%d",
            len(backlog), int(args.limit),
        )
        return _process_batch(service, limit=int(args.limit))
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
