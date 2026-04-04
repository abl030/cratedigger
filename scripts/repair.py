#!/usr/bin/env python3
"""Repair/orphan-recovery CLI — detect and fix inconsistent pipeline DB state.

Usage:
    repair.py scan [--dsn DSN]     # dry-run: show inconsistencies
    repair.py fix  [--dsn DSN]     # apply suggested repairs
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.pipeline_db import PipelineDB
from lib.quality import find_inconsistencies, suggest_repair
from lib.transitions import apply_transition

DEFAULT_DSN = os.environ.get(
    "PIPELINE_DB_DSN",
    "postgresql://soularr@192.168.100.11:5432/soularr",
)


def cmd_scan(db: PipelineDB) -> list:
    """Scan for inconsistencies and print them."""
    rows = _get_all_rows(db)
    issues = find_inconsistencies(rows)

    if not issues:
        print("No inconsistencies found.")
        return []

    print(f"Found {len(issues)} inconsistency(ies):\n")
    for issue in issues:
        repair = suggest_repair(issue)
        print(f"  [{issue.request_id}] {issue.issue_type}: {issue.detail}")
        print(f"         → suggested: {repair.action} — {repair.detail}")
        print()

    return issues


def cmd_fix(db: PipelineDB) -> None:
    """Apply suggested repairs."""
    rows = _get_all_rows(db)
    issues = find_inconsistencies(rows)

    if not issues:
        print("No inconsistencies found. Nothing to fix.")
        return

    print(f"Fixing {len(issues)} inconsistency(ies):\n")
    for issue in issues:
        repair = suggest_repair(issue)
        if repair.action == "reset_to_wanted":
            apply_transition(db, issue.request_id, "wanted",
                             from_status="downloading")
            print(f"  [{issue.request_id}] Reset to wanted (was corrupt downloading)")
        elif repair.action == "clear_imported_path":
            db._execute(
                "UPDATE album_requests SET imported_path = NULL, updated_at = NOW() "
                "WHERE id = %s",
                (issue.request_id,),
            )
            print(f"  [{issue.request_id}] Cleared stale imported_path")
        else:
            print(f"  [{issue.request_id}] Skipped: {repair.action} (manual review required)")


def _get_all_rows(db: PipelineDB) -> list:
    """Fetch all album_requests rows for inspection."""
    cur = db._execute(
        "SELECT id, status, active_download_state, imported_path "
        "FROM album_requests ORDER BY id"
    )
    return [dict(r) for r in cur.fetchall()]


def main():
    parser = argparse.ArgumentParser(description="Pipeline repair tool")
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("scan", help="Dry-run: show inconsistencies")
    sub.add_parser("fix", help="Apply suggested repairs")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    db = PipelineDB(args.dsn, run_migrations=False)
    try:
        if args.command == "scan":
            cmd_scan(db)
        elif args.command == "fix":
            cmd_fix(db)
    finally:
        db.close()


if __name__ == "__main__":
    main()
