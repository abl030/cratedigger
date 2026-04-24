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

from lib.config import read_runtime_config
from lib.download_recovery import (find_blocked_processing_path_issues,
                                   find_blocked_recovery_issues)
from lib.import_dispatch import transition_request
from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE, PipelineDB,
                             release_id_to_lock_key)
from lib.processing_paths import directory_has_entries
from lib.quality import (OrphanInfo, find_inconsistencies,
                         find_orphaned_downloads, suggest_repair)

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


def _dedupe_issues(issues: list[OrphanInfo]) -> list[OrphanInfo]:
    """Return issues with duplicate (request_id, type, detail) rows removed."""
    seen: set[tuple[int, str, str]] = set()
    deduped: list[OrphanInfo] = []
    for issue in issues:
        key = (issue.request_id, issue.issue_type, issue.detail)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(issue)
    return deduped


def _auto_import_in_progress(
    db: PipelineDB,
    request_id: int,
    mb_release_id: str | None,
) -> bool | None:
    """Return True when another session currently holds the release lock."""
    if not mb_release_id:
        return False
    try:
        cur = db._execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype = 'advisory'
                  AND classid = %s
                  AND objid = %s
                  AND mode = 'ExclusiveLock'
                  AND granted
                  AND database = (
                      SELECT oid
                      FROM pg_database
                      WHERE datname = current_database()
                  )
            ) AS held
            """,
            (
                ADVISORY_LOCK_NAMESPACE_RELEASE,
                release_id_to_lock_key(mb_release_id),
            ),
        )
        row = cur.fetchone()
        if isinstance(row, dict):
            return bool(row.get("held"))
        if row:
            return bool(row[0])
        return False
    except Exception as e:
        print(
            "  slskd: could not probe auto-import lock for "
            f"request {request_id}: {e}",
        )
        return None


def _collect_issues(db: PipelineDB, slskd_host: str | None,
                    slskd_key: str | None) -> list:
    """Collect all issues: DB inconsistencies + optional orphaned downloads."""
    rows = _get_all_rows(db)
    issues = find_inconsistencies(rows)
    if slskd_host and slskd_key:
        try:
            active = _get_slskd_active_transfers(slskd_host, slskd_key)
        except Exception as e:
            print(f"  slskd: could not check orphans: {e}")
            return _dedupe_issues(issues)

        orphans = find_orphaned_downloads(
            rows,
            active,
            existing_local_paths=None,
        )
        issues.extend(orphans)

        try:
            cfg = read_runtime_config()
        except Exception as e:
            print(f"  slskd: could not load runtime config for local-path checks: {e}")
            return _dedupe_issues(issues)

        blocked_processing_path_issues: list[OrphanInfo] = []
        blocked_recovery_issues: list[OrphanInfo] = []
        local_path_scan_failed = False
        try:
            blocked_processing_path_issues = [
                OrphanInfo(
                    request_id=issue.request_id,
                    issue_type="blocked_post_move",
                    detail=issue.detail,
                )
                for issue in find_blocked_processing_path_issues(
                    rows,
                    active,
                    staging_dir=cfg.beets_staging_dir,
                    slskd_download_dir=cfg.slskd_download_dir,
                    has_entries=directory_has_entries,
                    auto_import_in_progress=(
                        lambda request_id, mb_release_id: _auto_import_in_progress(
                            db,
                            request_id,
                            mb_release_id,
                        )
                    ),
                )
            ]
        except Exception as e:
            print(f"  slskd: could not inspect local recovery paths: {e}")
            local_path_scan_failed = True

        try:
            blocked_recovery_issues = [
                OrphanInfo(
                    request_id=issue.request_id,
                    issue_type="blocked_recovery",
                    detail=issue.detail,
                )
                for issue in find_blocked_recovery_issues(
                    rows,
                    active,
                    staging_dir=cfg.beets_staging_dir,
                    slskd_download_dir=cfg.slskd_download_dir,
                    has_entries=directory_has_entries,
                )
            ]
        except Exception as e:
            print(f"  slskd: could not inspect local recovery paths: {e}")
            local_path_scan_failed = True

        issues.extend(blocked_processing_path_issues)
        issues.extend(blocked_recovery_issues)
        issues = _dedupe_issues(issues)
        if (
            not local_path_scan_failed
            and not orphans
            and not blocked_processing_path_issues
            and not blocked_recovery_issues
        ):
            print(f"  slskd: checked {len(active)} active transfers, no orphans.")
        return issues

    downloading = [r for r in rows if r["status"] == "downloading"
                   and r.get("active_download_state")]
    if downloading:
        print(f"  Note: {len(downloading)} downloading row(s) — pass "
              "--slskd-host/--slskd-key to check for orphans.")
    return _dedupe_issues(issues)


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
            transition_request(
                db,
                issue.request_id,
                "wanted",
                from_status="downloading",
                message="Repair reset orphaned download to wanted",
            )
            print(f"  [{issue.request_id}] Reset to wanted ({issue.issue_type})")
        else:
            print(f"  [{issue.request_id}] Skipped: {repair.action} (manual review required)")


def _get_all_rows(db: PipelineDB) -> list:
    """Fetch all album_requests rows for inspection."""
    cur = db._execute(
        "SELECT id, status, artist_name, album_title, year, mb_release_id, "
        "active_download_state, imported_path "
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
