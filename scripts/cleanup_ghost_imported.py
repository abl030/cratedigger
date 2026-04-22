#!/usr/bin/env python3
"""Find and optionally purge ghost imported pipeline rows.

A ghost imported row is ``album_requests.status='imported'`` but the
exact release ID is absent from the beets library. This script uses the
same exact-ID seam as the web UI (`BeetsDB.locate`) and can either print
the rows (`--dry-run`, default) or delete them from the pipeline DB
(`--apply`).
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import BeetsDB
from lib.pipeline_db import PipelineDB

DEFAULT_DSN = os.environ.get(
    "PIPELINE_DB_DSN",
    "postgresql://cratedigger@192.168.100.11:5432/cratedigger",
)


def _row_release_id(row: dict[str, Any]) -> str:
    return str(row.get("mb_release_id") or row.get("discogs_release_id") or "").strip()


def classify_imported_rows(
    rows: list[dict[str, Any]],
    beets: BeetsDB,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Split imported rows into ghosts and manual-review rows."""
    ghosts: list[dict[str, Any]] = []
    manual_review: list[dict[str, Any]] = []
    for row in rows:
        release_id = _row_release_id(row)
        if not release_id:
            manual_review.append(row)
            continue
        if beets.locate(release_id).kind == "absent":
            ghosts.append(row)
    return ghosts, manual_review


def _print_rows(label: str, rows: list[dict[str, Any]]) -> None:
    print(f"{label}: {len(rows)}")
    for row in rows:
        release_id = _row_release_id(row)
        imported_path = row.get("imported_path") or ""
        print(
            f"{row['id']}\t{row['artist_name']} - {row['album_title']}\t"
            f"{release_id}\t{imported_path}"
        )


def cmd_scan(db: PipelineDB, beets: BeetsDB) -> int:
    rows = db.get_by_status("imported")
    ghosts, manual_review = classify_imported_rows(rows, beets)
    _print_rows("ghost imported rows", ghosts)
    if manual_review:
        print()
        _print_rows("manual review needed (no release id)", manual_review)
    return 0


def cmd_apply(db: PipelineDB, beets: BeetsDB) -> int:
    rows = db.get_by_status("imported")
    ghosts, manual_review = classify_imported_rows(rows, beets)
    for row in ghosts:
        db.delete_request(int(row["id"]))
        print(f"deleted {row['id']}: {row['artist_name']} - {row['album_title']}")
    print(f"\ndeleted {len(ghosts)} ghost imported rows")
    if manual_review:
        print()
        _print_rows("manual review needed (no release id)", manual_review)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=DEFAULT_DSN,
                        help="PostgreSQL DSN for the pipeline DB")
    parser.add_argument("--beets-db", default="/mnt/virtio/Music/beets-library.db",
                        help="Path to beets-library.db")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="Print ghost rows without deleting them (default)")
    mode.add_argument("--apply", action="store_true",
                      help="Delete matching ghost imported rows")
    args = parser.parse_args()

    db = PipelineDB(args.dsn)
    try:
        with BeetsDB(args.beets_db) as beets:
            if args.apply:
                return cmd_apply(db, beets)
            return cmd_scan(db, beets)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
