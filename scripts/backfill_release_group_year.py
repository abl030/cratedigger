#!/usr/bin/env python3
"""Backfill ``album_requests.release_group_year`` from the local MB mirror.

U3 / R9 of the search-plan-entropy plan. The migration
``026_album_requests_release_group_year.sql`` adds the column as
nullable. This script populates it for every request that has an
``mb_release_group_id`` but no ``release_group_year``, by fetching the
release-group's ``first-release-date`` from the local MB mirror
(``web/mb.py::get_release_group_year``) and parsing the year prefix.

MB mirror response shape (verified 2026-05-19 against the local mirror
at 192.168.1.35:5200): ``GET /ws/2/release-group/<mbid>?fmt=json``
returns ``{"first-release-date": "YYYY-MM-DD", ...}`` directly on the
release-group document. No need to paginate child releases and derive
``min(release.date)`` — one round-trip per request.

Idempotency: the ``WHERE release_group_year IS NULL`` filter on
``get_requests_missing_release_group_year`` means re-running the script
skips rows already populated. A 404 from the mirror leaves the row
NULL (logged but not failed) so a future re-run can retry.

Batching: requests are processed in chunks of ``BATCH_SIZE``. Each
chunk fetches its rows up front, then ``set_release_group_year`` is
called per row. The script logs a counter every batch. A SIGINT
between batches loses at most the in-flight chunk; everything before
is persisted.

Usage:
    nix-shell --run "python3 scripts/backfill_release_group_year.py"
    nix-shell --run "python3 scripts/backfill_release_group_year.py --dry-run"
    nix-shell --run "python3 scripts/backfill_release_group_year.py --limit 100"
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from typing import Callable

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

logger = logging.getLogger("backfill_release_group_year")

BATCH_SIZE = 500
RATE_LIMIT_SECONDS = 0.0  # local mirror — no throttle by default


# Result counters — surfaced in the final summary and (post-deploy) in
# the systemd unit log so operators can confirm convergence.
class BackfillCounters:
    def __init__(self) -> None:
        self.fetched: int = 0
        self.populated: int = 0
        self.not_found: int = 0  # mirror 404 — row stays NULL
        self.no_date: int = 0    # mirror returned no parseable year
        self.errors: int = 0     # unexpected exception per row


def run_backfill(
    *,
    db,
    fetch_year: Callable[[str], int | None],
    batch_size: int = BATCH_SIZE,
    limit: int | None = None,
    dry_run: bool = False,
    sleep_seconds: float = RATE_LIMIT_SECONDS,
) -> BackfillCounters:
    """Iterate requests needing ``release_group_year`` and populate them.

    ``db`` may be a ``PipelineDB`` or a ``FakePipelineDB`` — both
    expose ``get_requests_missing_release_group_year`` and
    ``set_release_group_year``.

    ``fetch_year`` is injected (rather than imported) so tests can pass
    a stub mapping ``rg_mbid -> int | None``. Production passes
    ``web.mb.get_release_group_year``.

    Returns a populated ``BackfillCounters`` summary.
    """
    counters = BackfillCounters()
    remaining = limit

    while True:
        chunk_size = batch_size
        if remaining is not None:
            if remaining <= 0:
                break
            chunk_size = min(batch_size, remaining)

        rows = db.get_requests_missing_release_group_year(limit=chunk_size)
        if not rows:
            break

        for row in rows:
            counters.fetched += 1
            rg_mbid = row["mb_release_group_id"]
            label = f"id={row['id']} {row['artist_name']} - {row['album_title']}"
            try:
                year = fetch_year(rg_mbid)
            except Exception:
                counters.errors += 1
                logger.exception(
                    "release-group-year fetch failed for %s rg=%s",
                    label, rg_mbid,
                )
                # Leave the row NULL — next run will retry. Do not raise:
                # one bad release-group must not abort the whole backfill.
                year = None
                if sleep_seconds:
                    time.sleep(sleep_seconds)
                continue

            if year is None:
                # Distinguish "MB mirror returned 404 / no first-release-date"
                # from real exceptions above. The bucket is informational —
                # both leave the row NULL.
                counters.no_date += 1
                logger.info(
                    "release-group %s has no first-release year for %s; "
                    "leaving release_group_year NULL", rg_mbid, label,
                )
                if sleep_seconds:
                    time.sleep(sleep_seconds)
                continue

            counters.populated += 1
            if not dry_run:
                db.set_release_group_year(row["id"], year)
            logger.info("%s -> release_group_year=%d", label, year)

            if sleep_seconds:
                time.sleep(sleep_seconds)

        if remaining is not None:
            remaining -= len(rows)

        # If the chunk was smaller than the requested size, the WHERE
        # clause is exhausted (or the limit was reached). Either way,
        # the next iteration would return zero rows — break early.
        if len(rows) < chunk_size:
            break

    return counters


def _build_parser() -> argparse.ArgumentParser:
    description = (__doc__ or "").splitlines()[0] if __doc__ else "backfill"
    p = argparse.ArgumentParser(description=description)
    p.add_argument(
        "--dsn",
        default=os.environ.get(
            "PIPELINE_DB_DSN",
            "postgresql://cratedigger@192.168.100.11:5432/cratedigger",
        ),
        help="PostgreSQL DSN for the pipeline DB",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="Process at most this many rows total (debug/staging)",
    )
    p.add_argument(
        "--batch-size", type=int, default=BATCH_SIZE,
        help=f"Rows fetched per chunk (default {BATCH_SIZE})",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Fetch + classify but do not write back to the DB",
    )
    p.add_argument(
        "--verbose", "-v", action="store_true",
        help="DEBUG-level logging",
    )
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    from lib.pipeline_db import PipelineDB
    from web.mb import get_release_group_year

    db = PipelineDB(args.dsn)
    try:
        counters = run_backfill(
            db=db,
            fetch_year=get_release_group_year,
            batch_size=args.batch_size,
            limit=args.limit,
            dry_run=args.dry_run,
        )
    finally:
        db.close()

    logger.info(
        "backfill complete: fetched=%d populated=%d no_date=%d errors=%d (dry_run=%s)",
        counters.fetched, counters.populated, counters.no_date,
        counters.errors, args.dry_run,
    )
    # Exit non-zero only if every fetched row errored — partial errors
    # are expected (some release-groups have no date) and should not
    # fail the deploy.
    if counters.fetched > 0 and counters.errors == counters.fetched:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
