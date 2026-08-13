"""Read-only cross-engine audit commands."""

from __future__ import annotations

import argparse
import json
from typing import TYPE_CHECKING

import msgspec

from lib.beets_db import BeetsDB, open_beets_db
from lib.retag_divergence_audit import (
    RetagDivergenceReport,
    scan_retag_divergence_from_factory,
)
from lib.world_audit_service import WorldAuditReport, audit_world_from_factory

if TYPE_CHECKING:
    from lib.world_audit_service import WorldAuditPipelineDB


class _AuditWorldArgs(msgspec.Struct, frozen=True):
    beets_db: str | None = None
    beets_directory: str | None = None
    json: bool = False


class _AuditRetagDivergenceArgs(msgspec.Struct, frozen=True):
    beets_db: str | None = None
    beets_directory: str | None = None
    json: bool = False


def _open_beets(path: str | None, library_root: str | None) -> BeetsDB:
    return open_beets_db(db_path=path, library_root=library_root)


def _add_beets_override_args(parser: argparse.ArgumentParser) -> None:
    """Shared ``--beets-db``/``--beets-directory``/``--json`` trio every
    read-only audit subcommand under ``audit`` exposes."""
    parser.add_argument(
        "--beets-db",
        default=None,
        help="Explicit Beets SQLite override; requires --beets-directory.",
    )
    parser.add_argument(
        "--beets-directory",
        default=None,
        help="Library root paired with --beets-db.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit the shared machine-readable report.",
    )


def _render_text(report: WorldAuditReport) -> None:
    counts = report.counts
    print(f"world audit: {report.status}")
    print(
        "counts: "
        f"active_requests={counts.active_requests} "
        f"beets_albums={counts.beets_albums} "
        f"linked_evidence={counts.linked_evidence} "
        f"denylist_rows={counts.denylist_rows}"
    )
    print(f"complete: {'yes' if report.complete else 'no'}")
    print("audited invariants: " + ", ".join(report.audited_invariants))
    print(
        "temporal invariants not auditable from current state: "
        + ", ".join(report.temporal_invariants_not_auditable)
    )
    for group in (report.groups.a, report.groups.b, report.groups.c):
        print(
            f"bucket {group.bucket} ({group.owner}): "
            f"{group.count}"
        )
        for violation in group.members:
            print(f"  {violation.code}: {violation.detail}")


def _render_retag_divergence_text(report: RetagDivergenceReport) -> None:
    counts = report.counts
    print(f"retag divergence audit: {report.status}")
    print(
        "counts: "
        f"albums_scanned={counts.albums_scanned} "
        f"items_read={counts.items_read} "
        f"items_unreadable={counts.items_unreadable} "
        f"albums_diverging={counts.albums_diverging} "
        f"albums_file_tag_present_db_absent="
        f"{counts.albums_file_tag_present_db_absent} "
        f"albums_unreadable={counts.albums_unreadable} "
        f"albums_empty={counts.albums_empty}"
    )
    print(f"complete: {'yes' if report.complete else 'no'}")
    if report.unavailable_detail:
        print(f"unavailable: {report.unavailable_detail}")
    for album in report.albums:
        print(
            f"album {album.album_id} ({album.album_class}): "
            f"db_mb_albumid={album.db_mb_albumid!r} "
            f"item_count={album.item_count}"
        )
        for item in album.items:
            detail = f" — {item.detail}" if item.detail else ""
            print(
                f"  {item.item_class}: {item.path} "
                f"(file_mb_albumid={item.file_mb_albumid!r}){detail}"
            )


def cmd_audit_retag_divergence(db: object, args: object) -> int:
    """Census the retag ``-W`` divergence cohort (#1093 item 1).

    Read-only over Beets alone — ``db`` (the pipeline DB connection every
    ``audit`` subcommand's dispatch dict is called with) is unused.

    Exit 1 iff ``status == "divergence_found"`` — a genuine identity
    mismatch, the one thing this instrument exists to surface. Exit 5 iff
    ``status == "beets_unavailable"`` — the audit never actually ran, so
    exit 0 there would let a cron or `&& echo "cohort empty"` read "no
    divergence" from a report that answered nothing; `SQLITE_BUSY`/
    `SQLITE_LOCKED`/`SQLITE_CANTOPEN`, `PermissionError`, and
    `FileNotFoundError` are exactly the transient/retryable class
    `.claude/rules/code-quality.md` § CLI ⇄ API Surface Symmetry maps to
    5/503 (#1093 review round 3, finding 1). ``clean`` and ``incomplete``
    both exit 0 — an incomplete scan (unreadable/empty/refused-only
    findings, or a truncated deadline) is not itself a finding, only a
    genuine divergence is. NOTE: `cmd_audit_world` still exits 0 for its
    own analogous beets-unavailable bucket — a pre-existing deviation from
    the same documented convention, deliberately left alone here (see the
    PR body / post-ship reflection) as an existing command's contract
    change outside this issue's scope.

    Exit 5 also covers an unexpected transport/decode/render defect — the
    whole body below runs inside the try so a
    `msgspec.convert`/serialization failure can't traceback out as an
    unmapped exit 1 (#1093 review round 2, finding 5; the previous shape
    left the argument decode and the render/json-encode steps outside the
    try).
    """
    del db
    try:
        typed_args = msgspec.convert(vars(args), type=_AuditRetagDivergenceArgs)
        report = scan_retag_divergence_from_factory(
            lambda: _open_beets(
                typed_args.beets_db,
                typed_args.beets_directory,
            ),
        )
        if typed_args.json:
            print(json.dumps(msgspec.to_builtins(report), indent=2))
        else:
            _render_retag_divergence_text(report)
    except Exception as exc:  # noqa: BLE001 - transport boundary, not typed B
        print(json.dumps({
            "error": "retag_divergence_audit_failed",
            "detail": str(exc),
        }))
        return 5
    if report.status == "divergence_found":
        return 1
    if report.status == "beets_unavailable":
        return 5
    return 0


def cmd_audit_world(db: WorldAuditPipelineDB, args: object) -> int:
    """Run the shared world invariant bank without mutating either store.

    The whole body runs inside the try so a `msgspec.convert`/
    serialization defect can't traceback out as an unmapped exit 1 instead
    of the documented exit 5 (#1093 review round 3, finding 2 — the same
    defect class as `cmd_audit_retag_divergence`'s finding-5 fix, in the
    same function family in the same file; the previous shape here left
    the argument decode and the render/json-encode steps outside the try).
    """
    try:
        typed_args = msgspec.convert(vars(args), type=_AuditWorldArgs)
        report = audit_world_from_factory(
            db,
            lambda: _open_beets(
                typed_args.beets_db,
                typed_args.beets_directory,
            ),
        )
        if typed_args.json:
            print(json.dumps(msgspec.to_builtins(report), indent=2))
        else:
            _render_text(report)
    except Exception as exc:  # noqa: BLE001 - transport boundary, not typed B
        print(json.dumps({
            "error": "world_audit_failed",
            "detail": str(exc),
        }))
        return 5
    return 1 if report.status == "integrity_failed" else 0


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
    _add_beets_override_args(world)
    retag_divergence = operations.add_parser(
        "retag-divergence",
        help=(
            "Census albums whose Beets DB identity moved but whose "
            "installed file tags did not (#1093 item 1)."
        ),
    )
    _add_beets_override_args(retag_divergence)


__all__ = [
    "add_audit_subparser",
    "cmd_audit_retag_divergence",
    "cmd_audit_world",
]
