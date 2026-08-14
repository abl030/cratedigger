#!/usr/bin/env python3
"""Daily whole-library retag-divergence census oneshot (#1142).

Runs as `cratedigger-retag-census.service`, scheduled by
`cratedigger-retag-census.timer` (`OnCalendar=daily`) — completely
separate from the 5-min `cratedigger.service` loop, mirroring
`scripts/run_unfindable_detection.py`'s own daily-oneshot shape.

Wires the runtime config to a real Beets authority, runs the UNBOUNDED
whole-library census (`lib.retag_divergence_audit.
scan_retag_divergence_from_factory`, no deadline — the same call
`pipeline-cli audit retag-divergence` makes), and atomically publishes
the result as a `RetagDivergenceCensusSnapshot`
(`lib.retag_divergence_census_snapshot`). The dashboard route and the
per-album recheck both read that persisted snapshot; neither ever
triggers a scan of their own — the ~93,700-file / ~200s cost stays here,
off the request/render path.

Run-health / exit code:
  * ``0`` — the census ran to completion and a snapshot WAS published.
    Covers every `report.status` the scan itself considers a real,
    computed answer worth persisting: ``clean``, ``divergence_found``,
    and ``incomplete`` (the last one is real production territory here
    — an unreadable file anywhere in the library makes the WHOLE-library
    scan's own status ``incomplete`` even though nothing truncated or
    resumed it; the daily writer still publishes that report, since it
    IS a genuine answer the dashboard should show, just not a clean
    one). Exit 0 says "the job did its job", not "the library is clean"
    — read the published report's own `status` for that.
  * ``EXIT_BEETS_UNAVAILABLE`` (1) — the Beets authority was unreachable
    this run. NOTHING is published (#1142 review B1): a
    `beets_unavailable` report never actually scanned anything, so
    publishing it would silently replace yesterday's real answer with a
    fabricated all-zero "nothing wrong" snapshot — worse than simply
    leaving the last real answer in place. Still an operationally
    visible failure worth failing the systemd unit over, so an operator
    notices a stuck/misconfigured Beets authority.
  * ``EXIT_CONFIG_ABORT`` (2) — the runtime config or Beets contract was
    rejected before any scan ran; no snapshot attempted.
  * ``EXIT_RUN_FAILED`` (3) — an unexpected (non-beets-availability)
    exception escaped the scan or the publish step. No snapshot is
    written in this case — `write_retag_divergence_census_snapshot`'s
    atomic same-directory-temp-file + `os.replace` means a raised
    exception, wherever it occurs, always leaves any PRIOR snapshot at
    the published path untouched (acceptance criterion 1).
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from lib.beets_db import open_beets_db
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import resolve_startup_config_paths
from lib.retag_divergence_audit import (
    RetagDivergenceBeetsFactory,
    scan_retag_divergence_from_factory,
)
from lib.retag_divergence_census_snapshot import (
    RetagDivergenceCensusSnapshot,
    retag_divergence_census_snapshot_path,
    write_retag_divergence_census_snapshot,
)

logger = logging.getLogger("cratedigger-retag-census")

# Distinct from EXIT_CONFIG_ABORT/EXIT_RUN_FAILED — see module docstring.
EXIT_BEETS_UNAVAILABLE = 1
EXIT_CONFIG_ABORT = 2
EXIT_RUN_FAILED = 3


def _default_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def run_retag_divergence_census(
    beets_factory: RetagDivergenceBeetsFactory,
    *,
    time_fn: Callable[[], float] = time.monotonic,
    now_fn: Callable[[], str] = _default_now_iso,
) -> RetagDivergenceCensusSnapshot:
    """Run the unbounded whole-library census and wrap it as a snapshot.

    Pure composition, no filesystem write — propagates any unexpected
    (non-beets-availability) exception exactly like
    `scan_retag_divergence_from_factory` itself does; an expected
    unavailable-Beets condition is not an exception here at all, it is
    the normal `report.status == "beets_unavailable"` outcome.
    """
    generated_at = now_fn()
    started = time_fn()
    report = scan_retag_divergence_from_factory(beets_factory)
    duration = time_fn() - started
    return RetagDivergenceCensusSnapshot(
        generated_at=generated_at, duration_seconds=duration, report=report,
    )


def publish_retag_divergence_census(
    path: str,
    beets_factory: RetagDivergenceBeetsFactory,
    *,
    time_fn: Callable[[], float] = time.monotonic,
    now_fn: Callable[[], str] = _default_now_iso,
) -> RetagDivergenceCensusSnapshot:
    """Run the census and atomically publish it at ``path`` — UNLESS the
    report itself says Beets was unavailable this run (#1142 review B1).

    A ``beets_unavailable`` report never actually scanned anything — its
    counts are all zero and its album list is empty, which is
    indistinguishable, on the dashboard, from a genuinely clean
    93,000-item library. Publishing it would silently replace the last
    REAL answer with a fabricated "nothing wrong" one, hiding a stuck or
    misconfigured Beets authority behind a green card. Nothing is
    written in that case; any snapshot already at ``path`` is left
    exactly as it was. The RETURNED snapshot still carries the
    ``beets_unavailable`` report either way, so the caller (``main``) can
    log the outcome.

    A raised exception — from the scan itself, or from the atomic write
    when the report WAS publishable — likewise leaves any snapshot
    already at ``path`` untouched (acceptance criterion 1):
    `write_retag_divergence_census_snapshot` never writes a partial
    file, and this function attempts the write only after the scan has
    fully completed.
    """
    snapshot = run_retag_divergence_census(
        beets_factory, time_fn=time_fn, now_fn=now_fn,
    )
    if snapshot.report.status != "beets_unavailable":
        write_retag_divergence_census_snapshot(path, snapshot)
    return snapshot


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Cratedigger daily retag-divergence census oneshot",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Immutable runtime config (default: env or cwd/config.ini)",
    )
    parser.add_argument(
        "--runtime-dir",
        default=None,
        help="Mutable runtime directory (default: cwd)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    config_path, runtime_dir = resolve_startup_config_paths(
        config_path=args.config, runtime_dir=args.runtime_dir,
    )
    try:
        cfg = enforce_beets_startup(
            role="web",
            config_path=config_path,
            runtime_dir=runtime_dir,
            logger=logger,
        )
    except BeetsStartupError:
        return EXIT_CONFIG_ABORT

    path = retag_divergence_census_snapshot_path(cfg.var_dir)
    try:
        snapshot = publish_retag_divergence_census(
            path, lambda: open_beets_db(config=cfg),
        )
    except Exception:
        logger.exception(
            "retag_divergence_census: run failed unexpectedly; any prior "
            "snapshot at %s is preserved",
            path,
        )
        return EXIT_RUN_FAILED

    report = snapshot.report
    if report.status == "beets_unavailable":
        logger.error(
            "retag_divergence_census: Beets authority unavailable (%s) "
            "this run; NOT publishing — any prior snapshot at %s is "
            "preserved",
            report.unavailable_detail, path,
        )
        return EXIT_BEETS_UNAVAILABLE
    logger.info(
        "retag_divergence_census: published status=%s albums_listed=%d "
        "albums_scanned=%d duration=%.1fs -> %s",
        report.status, len(report.albums), report.counts.albums_scanned,
        snapshot.duration_seconds, path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
