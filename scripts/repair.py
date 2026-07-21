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
from dataclasses import dataclass
from typing import Callable, Protocol

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib.config import read_runtime_config
from lib import transitions

# Module-level DI seam for ``transitions.finalize_request`` — see
# ``lib.dispatch.outcome_actions.finalize_request`` for the rationale.
finalize_request = transitions.finalize_request

from lib.download_recovery import (BlockedRecoveryIssue,
                                   find_blocked_processing_path_issues,
                                   find_blocked_recovery_issues)
from lib.pipeline_db import (ADVISORY_LOCK_NAMESPACE_RELEASE, PipelineDB,
                             release_id_to_lock_key)
from lib.processing_paths import directory_has_entries
from lib.repair import (OrphanInfo, SlskdOrphanTransfer, find_inconsistencies,
                        find_orphaned_downloads, find_slskd_orphans,
                        suggest_repair)
from lib.slskd_client import DownloadUser


class _CursorRows(Protocol):
    """Narrow shape of the cursor ``PipelineDB._execute`` returns — just
    enough for this module's raw-SQL probes (issue #784, #409 pattern;
    mirrors ``scripts/pipeline_cli/album_requests.py``). ``fetchone`` keeps
    the tuple-row branch typeable too — real ``_execute`` calls always use
    ``RealDictCursor`` so that branch is defensive/unreachable today, but
    this stays annotation-only and must not assert that away.
    """

    def fetchall(self) -> list[dict[str, object]]: ...
    def fetchone(self) -> dict[str, object] | tuple[object, ...] | None: ...


class _RepairDB(transitions.TransitionsDB, Protocol):
    """Narrow surface this module's scan/fix helpers touch through the
    shared ``db`` handle (issue #784, #409 pattern) -- ``TransitionsDB``
    (the status-transition engine's own narrow protocol, reused here since
    ``cmd_fix`` calls ``finalize_request``/``require_transition_applied``)
    plus the two extra raw-SQL / ledger methods this module calls
    directly. ``PipelineDB``'s real ``_execute`` is untyped in its own
    module (out of this migration's scope) — this Protocol carries the
    fully-typed contract instead of inheriting that gap.
    """

    def _execute(
        self, sql: str, params: tuple[object, ...] = (),
    ) -> _CursorRows: ...

    def get_owned_transfer_keys(self) -> set[tuple[str, str]]: ...

# No hardcoded fallback (#479): the nspawn DB has moved before (last time to
# 10.20.0.11) and a baked-in IP silently dials a dead host forever after the
# next move. Fail loud in main() instead of guessing.
DEFAULT_DSN = os.environ.get("PIPELINE_DB_DSN")


def _fetch_slskd_downloads(host: str, api_key: str) -> list[DownloadUser]:
    """Fetch the ``get_all_downloads()`` snapshot (live transfers only),
    already typed via ``lib.slskd_client.parse_downloads_envelope``
    (issue #507).

    Kept as its own network seam so ``_collect_issues`` can derive BOTH the
    forward orphan view (``_active_transfer_pairs``) and the inverse
    slskd-side orphan report (``lib.repair.find_slskd_orphans``) from one
    fetch, instead of flattening to pairs and discarding the raw structure
    the way this used to (#479 item 1).
    """
    from lib.slskd_client import SlskdClient
    client = SlskdClient(host=host, api_key=api_key)
    return client.transfers.get_all_downloads(includeRemoved=False)


def _active_transfer_pairs(downloads: list[DownloadUser]) -> set[tuple[str, str]]:
    """Flatten a slskd downloads snapshot to (username, filename) pairs. Pure."""
    pairs: set[tuple[str, str]] = set()
    for user_group in downloads:
        username = user_group.username
        for d in user_group.directories:
            for f in d.files:
                if f.filename:
                    pairs.add((username, f.filename))
    return pairs


@dataclass(frozen=True)
class CollectedIssues:
    """Result of ``_collect_issues`` (#479): actionable DB issues, plus a
    read-only report of slskd-side orphans (ledger-owned live transfers no
    ``downloading`` row backs — #571 PR 3 flipped this to ledger-positive
    ownership).

    ``slskd_orphans`` and ``slskd_foreign_count`` are informational only —
    ``cmd_fix`` never acts on either. The #278 convergence
    (``lib.slskd_transfers.converge_slskd_orphans``, run every cycle
    before search) is the only thing that ever cancels a transfer; it
    self-heals on its own next pass regardless of whether anyone reads
    this report. Both fields come from the SAME classification helper
    (``lib.repair.find_slskd_orphans``) production convergence uses, so
    this report can never diverge from what convergence will actually do.
    """
    issues: list[OrphanInfo]
    slskd_orphans: list[SlskdOrphanTransfer]
    slskd_foreign_count: int = 0


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
    db: _RepairDB,
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
                  AND objsubid = 2
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


def _blocked_processing_issue_type(detail: str) -> str:
    if "auto-abandonable request-scoped auto-import" in detail:
        return "auto_abandon_import"
    return "blocked_post_move"


def _collect_issues(
    db: _RepairDB,
    slskd_host: str | None,
    slskd_key: str | None,
    *,
    find_orphaned_fn: Callable[..., list[OrphanInfo]] = find_orphaned_downloads,
    find_blocked_recovery_fn: Callable[
        ..., list[BlockedRecoveryIssue]
    ] = find_blocked_recovery_issues,
) -> CollectedIssues:
    """Collect all issues: DB inconsistencies + optional orphaned downloads.

    The two ``find_*_fn`` kwargs are dependency-injection seams. Production
    leaves them at their default (the imported helpers); tests pass stubs
    to drive each branch without going through real slskd / filesystem
    fixtures.
    """
    rows = _get_all_rows(db)
    issues = find_inconsistencies(rows)
    if slskd_host and slskd_key:
        try:
            downloads = _fetch_slskd_downloads(slskd_host, slskd_key)
        except Exception as e:
            print(f"  slskd: could not check orphans: {e}")
            return CollectedIssues(issues=_dedupe_issues(issues), slskd_orphans=[])

        active = _active_transfer_pairs(downloads)
        # Inverse direction (#278/#479), ledger-positive since #571 PR 3:
        # live transfers cratedigger ledgered but no downloading row backs.
        # Report-only — nothing here ever cancels a transfer; the SAME
        # helper production convergence uses, so this report can't
        # classify foreign-vs-stray differently than the real sweep.
        ownership = find_slskd_orphans(
            downloads, rows, db.get_owned_transfer_keys())
        slskd_orphans = ownership.orphans
        slskd_foreign_count = ownership.foreign_count

        orphans = find_orphaned_fn(
            rows,
            active,
            existing_local_paths=None,
        )
        issues.extend(orphans)

        try:
            cfg = read_runtime_config()
        except Exception as e:
            print(f"  slskd: could not load runtime config for local-path checks: {e}")
            return CollectedIssues(
                issues=_dedupe_issues(issues), slskd_orphans=slskd_orphans,
                slskd_foreign_count=slskd_foreign_count)

        blocked_processing_path_issues: list[OrphanInfo] = []
        blocked_recovery_issues: list[OrphanInfo] = []
        local_path_scan_failed = False
        try:
            blocked_processing_path_issues = [
                OrphanInfo(
                    request_id=issue.request_id,
                    issue_type=_blocked_processing_issue_type(issue.detail),
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
                for issue in find_blocked_recovery_fn(
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
        return CollectedIssues(
            issues=issues, slskd_orphans=slskd_orphans,
            slskd_foreign_count=slskd_foreign_count)

    downloading = [r for r in rows if r["status"] == "downloading"
                   and r.get("active_download_state")]
    if downloading:
        print(f"  Note: {len(downloading)} downloading row(s) — pass "
              "--slskd-host/--slskd-key to check for orphans.")
    return CollectedIssues(issues=_dedupe_issues(issues), slskd_orphans=[])


def _print_slskd_orphan_report(
    slskd_orphans: list[SlskdOrphanTransfer],
    slskd_foreign_count: int = 0,
) -> None:
    """Report-only (#479 item 1; ledger-positive since #571 PR 3):
    ledger-owned live slskd transfers no ``downloading`` row backs. Never
    triggers a cancel — the #278 convergence
    (``lib.slskd_transfers.converge_slskd_orphans``) is the only thing
    that reaps these, on its own next cycle.

    ``slskd_foreign_count`` (live transfers absent from cratedigger's
    ledger entirely — never candidates for convergence, whatever their
    state or age) is surfaced too so an operator scanning a shared slskd
    instance can see both classifications at a glance.
    """
    if not slskd_orphans and not slskd_foreign_count:
        return
    if slskd_orphans:
        print(
            f"slskd-side orphans (read-only, {len(slskd_orphans)}): "
            "ledger-owned live transfer(s) with no owning downloading "
            "row. The #278 convergence cancels these automatically next "
            "cycle — no action needed here.\n"
        )
        for orphan in slskd_orphans:
            print(f"  user={orphan.username!r} file={orphan.filename!r} "
                  f"state={orphan.state!r} id={orphan.transfer_id}")
        print()
    if slskd_foreign_count:
        print(
            f"slskd-side foreign transfers (read-only, {slskd_foreign_count}): "
            "live transfer(s) not in cratedigger's ledger — never touched "
            "by convergence.\n"
        )


def cmd_scan(db: _RepairDB, slskd_host: str | None = None,
             slskd_key: str | None = None) -> list[OrphanInfo]:
    """Scan for inconsistencies and print them."""
    collected = _collect_issues(db, slskd_host, slskd_key)
    issues = collected.issues

    if not issues:
        print("No inconsistencies found.")
    else:
        print(f"Found {len(issues)} inconsistency(ies):\n")
        for issue in issues:
            repair = suggest_repair(issue)
            print(f"  [{issue.request_id}] {issue.issue_type}: {issue.detail}")
            print(f"         → suggested: {repair.action} — {repair.detail}")
            print()

    _print_slskd_orphan_report(collected.slskd_orphans, collected.slskd_foreign_count)
    return issues


def cmd_fix(db: _RepairDB, slskd_host: str | None = None,
            slskd_key: str | None = None) -> None:
    """Apply suggested repairs.

    Only ``collected.issues`` is actionable here. ``collected.slskd_orphans``
    is deliberately ignored — it's a read-only report (see ``cmd_scan``);
    the #278 convergence is the only path that ever cancels a slskd
    transfer.
    """
    issues = _collect_issues(db, slskd_host, slskd_key).issues

    if not issues:
        print("No inconsistencies found. Nothing to fix.")
        return

    print(f"Fixing {len(issues)} inconsistency(ies):\n")
    for issue in issues:
        repair = suggest_repair(issue)
        if repair.action == "reset_to_wanted":
            transitions.require_transition_applied(finalize_request(
                db,
                issue.request_id,
                transitions.RequestTransition.to_wanted(
                    from_status="downloading"),
            ))
            print(f"  [{issue.request_id}] Reset to wanted ({issue.issue_type})")
        elif repair.action == "wait_for_automatic_recovery":
            print(
                f"  [{issue.request_id}] Skipped: "
                "automatic recovery will handle this row"
            )
        else:
            print(f"  [{issue.request_id}] Skipped: {repair.action} (manual review required)")


def _get_all_rows(db: _RepairDB) -> list[dict[str, object]]:
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
    if not args.dsn:
        parser.error(
            "no DSN: set PIPELINE_DB_DSN or pass --dsn "
            "(no hardcoded fallback — issue #479)"
        )
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
