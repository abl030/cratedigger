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

from lib.config import read_runtime_config  # noqa: E402
from lib.slskd_client import SlskdClient  # noqa: E402
from lib.pipeline_db import DEFAULT_DSN, PipelineDB  # noqa: E402
from lib.unfindable_detection_service import (  # noqa: E402
    DEFAULT_BATCH_SIZE,
    RESULT_CATEGORISED,
    RESULT_DOWNGRADED,
    RESULT_NOT_DUE,
    RESULT_NO_CHANGE,
    RESULT_PROBE_FAILED,
    RESULT_REQUEST_NOT_FOUND,
    UnfindableDetectionService,
)

logger = logging.getLogger("cratedigger-unfindable")


def _build_slskd_client(cfg) -> SlskdClient:
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


def _summarise(results) -> dict[str, int]:
    counts: dict[str, int] = {}
    for r in results:
        counts[r.outcome] = counts.get(r.outcome, 0) + 1
    return counts


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
        results = service.categorise_due_batch(limit=int(args.limit))
        counts = _summarise(results)
        # Log per-row outcomes at INFO so journalctl grep is fruitful.
        for r in results:
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
                # Should never come back from the batch path — the
                # candidate list already filtered by cadence. Log if
                # it does so the operator notices the drift.
                logger.debug(
                    "not_due (unexpected from batch) request=%s",
                    r.request_id,
                )
            elif r.outcome == RESULT_REQUEST_NOT_FOUND:
                logger.warning(
                    "request_not_found request=%s (race with operator?)",
                    r.request_id,
                )
        logger.info(
            "unfindable_detection: complete; processed=%d outcomes=%s",
            len(results), counts,
        )
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
