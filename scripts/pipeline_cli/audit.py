"""Read-only cross-engine audit commands."""

from __future__ import annotations

import argparse
import json

import msgspec

from lib.beets_db import BeetsDB, DEFAULT_BEETS_DB
from lib.world_audit_service import WorldAuditReport, audit_world


def _beets_library_root() -> str:
    from lib.config import read_runtime_config

    return read_runtime_config().beets_directory


def _open_beets(path: str) -> BeetsDB:
    return BeetsDB(path, library_root=_beets_library_root())


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
    try:
        beets = _open_beets(args.beets_db)
    except FileNotFoundError as exc:
        print(json.dumps({
            "error": "beets_db_unavailable",
            "detail": str(exc),
        }))
        return 5
    with beets:
        report = audit_world(db, beets)
    if args.json:
        print(json.dumps(msgspec.to_builtins(report), indent=2))
    else:
        _render_text(report)
    return 0 if report.status == "clean" else 1


def add_audit_subparser(sub: argparse._SubParsersAction) -> None:
    audit = sub.add_parser(
        "audit",
        help="Run read-only cross-engine invariant audits.",
    )
    operations = audit.add_subparsers(dest="audit_command", required=True)
    world = operations.add_parser(
        "world",
        help="Audit PipelineDB, Beets, and library-disk coherence.",
    )
    world.add_argument("--beets-db", default=DEFAULT_BEETS_DB)
    world.add_argument(
        "--json",
        action="store_true",
        help="Emit the shared machine-readable report.",
    )


__all__ = ["add_audit_subparser", "cmd_audit_world"]
