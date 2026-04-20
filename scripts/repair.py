#!/usr/bin/env python3
"""Repair/orphan-recovery CLI — detect and fix inconsistent pipeline DB state.

Usage:
    repair.py scan [--dsn DSN]     # dry-run: show inconsistencies
    repair.py fix  [--dsn DSN]     # apply suggested repairs

Optionally checks for orphaned downloads (downloading rows whose slskd
transfers no longer exist) when --slskd-host and --slskd-key are provided.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.pipeline_db import PipelineDB
from lib.quality import find_inconsistencies, find_orphaned_downloads, suggest_repair
from lib.transitions import apply_transition

DEFAULT_DSN = os.environ.get(
    "PIPELINE_DB_DSN",
    "postgresql://cratedigger@192.168.100.11:5432/cratedigger",
)


def _get_slskd_active_transfers(host: str, api_key: str) -> set[tuple[str, str]]:
    """Fetch active (username, filename) pairs from slskd API."""
    import slskd_api
    client = slskd_api.SlskdClient(host=host, api_key=api_key)
    downloads: Any = client.transfers.get_all_downloads(includeRemoved=False)
    pairs: set[tuple[str, str]] = set()
    if not isinstance(downloads, list):
        return pairs
    for user_group in downloads:
        username = user_group.get("username", "")
        for d in user_group.get("directories", []):
            for f in d.get("files", []):
                fname = f.get("filename")
                if fname:
                    pairs.add((username, fname))
    return pairs


def _collect_issues(db: PipelineDB, slskd_host: str | None,
                    slskd_key: str | None) -> list:
    """Collect all issues: DB inconsistencies + optional orphaned downloads."""
    rows = _get_all_rows(db)
    issues = find_inconsistencies(rows)
    if slskd_host and slskd_key:
        try:
            active = _get_slskd_active_transfers(slskd_host, slskd_key)
            orphans = find_orphaned_downloads(rows, active)
            issues.extend(orphans)
            if not orphans:
                print(f"  slskd: checked {len(active)} active transfers, no orphans.")
        except Exception as e:
            print(f"  slskd: could not check orphans: {e}")
    else:
        downloading = [r for r in rows if r["status"] == "downloading"
                       and r.get("active_download_state")]
        if downloading:
            print(f"  Note: {len(downloading)} downloading row(s) — pass "
                  "--slskd-host/--slskd-key to check for orphans.")
    return issues


def cmd_scan(db: PipelineDB, slskd_host: str | None = None,
             slskd_key: str | None = None) -> list:
    """Scan for inconsistencies and print them."""
    issues = _collect_issues(db, slskd_host, slskd_key)

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


def cmd_fix(db: PipelineDB, slskd_host: str | None = None,
            slskd_key: str | None = None) -> None:
    """Apply suggested repairs."""
    issues = _collect_issues(db, slskd_host, slskd_key)

    if not issues:
        print("No inconsistencies found. Nothing to fix.")
        return

    print(f"Fixing {len(issues)} inconsistency(ies):\n")
    for issue in issues:
        repair = suggest_repair(issue)
        if repair.action == "reset_to_wanted":
            apply_transition(db, issue.request_id, "wanted",
                             from_status="downloading")
            print(f"  [{issue.request_id}] Reset to wanted ({issue.issue_type})")
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
    parser.add_argument("--slskd-host", default=os.environ.get("SLSKD_HOST"),
                        help="slskd API URL (e.g. http://localhost:5030)")
    parser.add_argument("--slskd-key", default=os.environ.get("SLSKD_API_KEY"),
                        help="slskd API key")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("scan", help="Dry-run: show inconsistencies")
    sub.add_parser("fix", help="Apply suggested repairs")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    db = PipelineDB(args.dsn)
    try:
        if args.command == "scan":
            cmd_scan(db, args.slskd_host, args.slskd_key)
        elif args.command == "fix":
            cmd_fix(db, args.slskd_host, args.slskd_key)
    finally:
        db.close()


if __name__ == "__main__":
    main()
