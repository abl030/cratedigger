"""Read-only cross-engine audit commands."""

from __future__ import annotations

import argparse
import json
from typing import Protocol, cast

import msgspec

from lib.beets_db import BeetsDB, open_beets_db
from lib.world_audit_service import WorldAuditReport, audit_world


class _AuditWorldArgs(Protocol):
    beets_db: str | None
    beets_directory: str | None
    json: bool


def _open_beets(path: str | None, library_root: str | None) -> BeetsDB:
    return open_beets_db(db_path=path, library_root=library_root)


def _render_text(report: WorldAuditReport) -> None:
    counts = report.counts
    print(f"world audit: {report.status}")
    print(
        "counts: "
        f"active_requests={counts.active_requests} "
        f"beets_albums={counts.beets_albums} "
        f"linked_evidence={counts.linked_evidence} "
        f"denylist_rows={counts.denylist_rows} "
        f"violations={counts.violations}"
    )
    print("audited invariants: " + ", ".join(report.audited_invariants))
    print(
        "temporal invariants not auditable from current state: "
        + ", ".join(report.temporal_invariants_not_auditable)
    )
    for violation in report.violations:
        print(f"{violation.code}: {violation.detail}")


def cmd_audit_world(db, args: argparse.Namespace) -> int:
    """Run the shared world invariant bank without mutating either store."""
    typed_args = cast(_AuditWorldArgs, args)
    try:
        beets = _open_beets(
            typed_args.beets_db,
            typed_args.beets_directory,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(json.dumps({
            "error": "beets_db_unavailable",
            "detail": str(exc),
        }))
        return 5
    with beets:
        report = audit_world(db, beets)
    if typed_args.json:
        print(json.dumps(msgspec.to_builtins(report), indent=2))
    else:
        _render_text(report)
    return 0 if report.status == "clean" else 1


def add_audit_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    audit = sub.add_parser(
        "audit",
        help="Run read-only cross-engine invariant audits.",
    )
    operations = audit.add_subparsers(dest="audit_command", required=True)
    world = operations.add_parser(
        "world",
        help="Audit PipelineDB, Beets, and library-disk coherence.",
    )
    world.add_argument(
        "--beets-db",
        default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    world.add_argument(
        "--beets-directory",
        default=None,
        help="Library root paired with --beets-db.",
    )
    world.add_argument(
        "--json",
        action="store_true",
        help="Emit the shared machine-readable report.",
    )


__all__ = ["add_audit_subparser", "cmd_audit_world"]
