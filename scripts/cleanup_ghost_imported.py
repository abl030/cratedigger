#!/usr/bin/env python3
"""Find and optionally purge ghost imported pipeline rows.

A ghost imported row is ``album_requests.status='imported'`` but the
exact release ID is absent from the Beets library. This script uses the
shared typed current-release resolver and sends ambiguous or invalid identity
states to manual review. It can either print ghosts (`--dry-run`, default) or
delete only typed-missing rows from the pipeline DB (`--apply`).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping, Sequence
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.beets_db import (
    BeetsDB,
    CurrentBeetsAmbiguous,
    CurrentBeetsMissing,
    open_beets_db,
)
from lib.pipeline_db import PipelineDB
from lib.release_identity import ReleaseIdentity

# No hardcoded fallback (#479): the nspawn DB has moved before (last time
# to 10.20.0.11) and a baked-in IP silently dials a dead host forever
# after the next move. Fail loud in main() instead of guessing.
DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN")


ImportedRow = Mapping[str, object]


def _row_identity(row: ImportedRow) -> ReleaseIdentity | None:
    return ReleaseIdentity.from_fields(
        row.get("mb_release_id"),
        row.get("discogs_release_id"),
    )


def _row_release_id(row: ImportedRow) -> str:
    identity = _row_identity(row)
    return identity.release_id if identity is not None else ""


def classify_imported_rows(
    rows: Sequence[ImportedRow],
    beets: BeetsDB,
) -> tuple[list[ImportedRow], list[ImportedRow]]:
    """Split imported rows using the typed current-library authority."""
    ghosts: list[ImportedRow] = []
    manual_review: list[ImportedRow] = []
    identified: list[tuple[ImportedRow, ReleaseIdentity]] = []
    for row in rows:
        identity = _row_identity(row)
        if identity is None:
            manual_review.append(row)
            continue
        identified.append((row, identity))

    resolutions = beets.resolve_current_releases([
        identity for _row, identity in identified
    ])
    for row, identity in identified:
        resolution = resolutions[identity]
        if isinstance(resolution, CurrentBeetsMissing):
            ghosts.append(row)
        elif isinstance(resolution, CurrentBeetsAmbiguous):
            manual_review.append(row)
    return ghosts, manual_review


def _print_rows(label: str, rows: Sequence[ImportedRow]) -> None:
    print(f"{label}: {len(rows)}")
    for row in rows:
        release_id = _row_release_id(row)
        print(
            f"{row['id']}\t{row['artist_name']} - {row['album_title']}\t"
            f"{release_id}"
        )


def cmd_scan(db: PipelineDB, beets: BeetsDB) -> int:
    rows = db.get_by_status("imported")
    ghosts, manual_review = classify_imported_rows(rows, beets)
    _print_rows("ghost imported rows", ghosts)
    if manual_review:
        print()
        _print_rows(
            "manual review needed (missing identity or ambiguous Beets state)",
            manual_review,
        )
    return 0


def cmd_apply(db: PipelineDB, beets: BeetsDB) -> int:
    rows = db.get_by_status("imported")
    ghosts, manual_review = classify_imported_rows(rows, beets)
    for row in ghosts:
        request_id = row["id"]
        if not isinstance(request_id, int):
            raise ValueError(f"invalid imported request id: {request_id!r}")
        db.delete_request(request_id)
        print(
            f"deleted {request_id}: "
            f"{row['artist_name']} - {row['album_title']}"
        )
    print(f"\ndeleted {len(ghosts)} ghost imported rows")
    if manual_review:
        print()
        _print_rows(
            "manual review needed (missing identity or ambiguous Beets state)",
            manual_review,
        )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=DEFAULT_DSN,
                        help="PostgreSQL DSN for the pipeline DB")
    parser.add_argument(
        "--beets-db", default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    parser.add_argument(
        "--beets-directory", default=None,
        help="Library root paired with --beets-db.",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true",
                      help="Print ghost rows without deleting them (default)")
    mode.add_argument("--apply", action="store_true",
                      help="Delete matching ghost imported rows")
    args = parser.parse_args()
    if not args.dsn:
        parser.error(
            "no DSN: set PIPELINE_DB_DSN or pass --dsn "
            "(no hardcoded fallback — issue #479)"
        )

    db = PipelineDB(args.dsn)
    try:
        with open_beets_db(
            db_path=args.beets_db,
            library_root=args.beets_directory,
        ) as beets:
            if args.apply:
                return cmd_apply(db, beets)
            return cmd_scan(db, beets)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
