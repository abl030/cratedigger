#!/usr/bin/env python3
"""One-shot backfill: populate ``album_requests.mb_release_group_id``
for legacy rows where it is NULL.

Rationale: rows added before the RG field was populated have NULL
``mb_release_group_id``. The Replace operator action (and other
RG-keyed surfaces) used to gate UI affordances on this column being
truthy, hiding the button entirely for ~4001 legacy rows. The picker
now lazy-resolves at click-time via
``POST /api/pipeline/<id>/resolve-rg``, so this backfill is *not*
required for correctness — it's an optimisation that converts the
first-click latency from "MB lookup + UPDATE" to "cache-served".

Safe to run on production:
- Read-only side effects: a 24h MB-mirror cache fill plus the
  UPDATE on a single nullable column. Idempotent and resumable
  (the WHERE clause naturally skips already-backfilled rows).
- Numeric Discogs ids are skipped (no MB release-group concept).
- MB-mirror 404 / lookup failure leaves the row NULL and moves on.

Usage:
    nix-shell --run "python3 scripts/backfill_release_groups.py [--limit N]"

Environment:
    PIPELINE_DB_DSN  — defaults to
                       postgresql://cratedigger@localhost/cratedigger
                       In production set via the sops-managed
                       /run/secrets/cratedigger-pgpass on doc2.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import uuid
from urllib.error import URLError

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.pipeline_db import PipelineDB
from web import mb as mb_api


logger = logging.getLogger("backfill_release_groups")


# MB-mirror transient errors that should NOT mark the row as
# permanently un-resolvable. We just log and move on; a future run can
# retry. Mirrors lib.mbid_replace_service._TRANSIENT_LOOKUP_EXCEPTIONS.
_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    URLError, TimeoutError, socket.timeout, ConnectionError,
    json.JSONDecodeError,
)


def _is_mb_uuid(value: str | None) -> bool:
    """``True`` iff ``value`` is a UUID-shaped string. Numeric Discogs
    ids fail this check and are skipped — they have no MB
    release-group concept."""
    if not value:
        return False
    try:
        uuid.UUID(str(value))
    except (ValueError, TypeError, AttributeError):
        return False
    return True


def _rows_to_backfill(db: PipelineDB, limit: int | None) -> list[dict]:
    sql = (
        "SELECT id, mb_release_id, source "
        "FROM album_requests "
        "WHERE mb_release_group_id IS NULL "
        "AND mb_release_id IS NOT NULL "
        "ORDER BY id"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    cur = db._execute(sql)
    return list(cur.fetchall())


def backfill(db: PipelineDB, *, limit: int | None = None) -> dict[str, int]:
    """Walk all eligible rows and backfill RG. Returns a summary dict."""
    rows = _rows_to_backfill(db, limit)
    total = len(rows)
    updated = 0
    skipped_non_uuid = 0
    skipped_no_rg = 0
    errored_transient = 0
    errored_other = 0

    logger.info("Found %d row(s) to scan", total)

    for i, row in enumerate(rows, start=1):
        rid = int(row["id"])
        mb_release_id = row.get("mb_release_id")
        source = row.get("source")

        if not _is_mb_uuid(mb_release_id):
            logger.info(
                "skip id=%d mb_release_id=%r source=%s "
                "(non-MB id; no release-group concept)",
                rid, mb_release_id, source,
            )
            skipped_non_uuid += 1
        else:
            try:
                data = mb_api.get_release(mb_release_id, fresh=False)
            except _TRANSIENT_EXCEPTIONS as exc:
                logger.warning(
                    "transient MB error id=%d mb_release_id=%s: %s",
                    rid, mb_release_id, exc,
                )
                errored_transient += 1
                data = None
            except Exception as exc:  # noqa: BLE001
                # 404 from the mirror (release deleted upstream),
                # malformed payload, anything else — log + leave NULL.
                logger.warning(
                    "lookup failed id=%d mb_release_id=%s: %s",
                    rid, mb_release_id, exc,
                )
                errored_other += 1
                data = None

            if data is not None:
                rg_id = data.get("release_group_id") if isinstance(data, dict) else None
                if not rg_id:
                    logger.info(
                        "no RG id=%d mb_release_id=%s "
                        "(MB returned no release_group_id)",
                        rid, mb_release_id,
                    )
                    skipped_no_rg += 1
                else:
                    db.update_request_fields(rid, mb_release_group_id=rg_id)
                    updated += 1

        if i % 100 == 0:
            logger.info(
                "progress: scanned=%d updated=%d skipped=%d errored=%d",
                i, updated,
                skipped_non_uuid + skipped_no_rg,
                errored_transient + errored_other,
            )

    return {
        "scanned": total,
        "updated": updated,
        "skipped_non_uuid": skipped_non_uuid,
        "skipped_no_rg": skipped_no_rg,
        "errored_transient": errored_transient,
        "errored_other": errored_other,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill mb_release_group_id for legacy album_requests rows",
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Only process the first N rows (debug / smoke test)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Verbose per-row logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    db = PipelineDB()
    summary = backfill(db, limit=args.limit)
    print()
    print("=" * 60)
    print("Backfill summary:")
    for key, val in summary.items():
        print(f"  {key:24s} {val}")
    print("=" * 60)


if __name__ == "__main__":
    main()
