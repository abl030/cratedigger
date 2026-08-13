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
    after_album_id: int | None = None


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
        f"items_scanned={counts.items_scanned} "
        f"items_refused={counts.items_refused} "
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
    if report.next_after_album_id is not None:
        print(
            "truncated: resume with "
            f"--after-album-id {report.next_after_album_id}"
        )
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
    mismatch, the one thing this instrument exists to surface. Exit 4 iff
    ``status == "incomplete"`` — the world blocked a complete answer
    (unreadable/empty/refused-only findings, or a truncated deadline); this
    is `.claude/rules/code-quality.md` § CLI ⇄ API Surface Symmetry's
    `409`/`4` "wrong state" slot, not success — the same "cron reads a
    silent 0 as clean" argument below applies here just as much as to
    ``beets_unavailable`` (#1093 review round 4, finding 5; a prior
    version of this function mapped ``incomplete`` to exit 0 with exactly
    that flawed reasoning quoted against itself). Exit 5 iff
    ``status == "beets_unavailable"`` — the audit never actually ran, so
    exit 0 there would let a cron or `&& echo "cohort empty"` read "no
    divergence" from a report that answered nothing; the SQLite/filesystem
    authority-unavailability class `lib/beets_db.py::
    beets_authority_availability_category` recognizes (every code in
    `_SQLITE_AUTHORITY_AVAILABILITY_CODES` — SQLITE_AUTH/BUSY/CANTOPEN/
    IOERR/LOCKED/PERM — plus `PermissionError`/`FileNotFoundError`) is
    exactly the transient/retryable class that table maps to 5/503 (#1093
    review round 3, finding 1). Only ``clean`` exits 0 — the audit ran and
    the cohort really is empty. NOTE: `cmd_audit_world` still exits 0 for
    its own analogous beets-unavailable bucket — a pre-existing deviation
    from the same documented convention, deliberately left alone here (see
    the PR body / post-ship reflection) as an existing command's contract
    change outside this issue's scope.

    Exit 5 also covers an unexpected transport/decode/render defect — the
    whole body below runs inside the try so a
    `msgspec.convert`/serialization failure can't traceback out as an
    unmapped exit 1 (#1093 review round 2, finding 5; the previous shape
    left the argument decode and the render/json-encode steps outside the
    try). A `BrokenPipeError` from a downstream reader (e.g. `| head`)
    closing early is caught SEPARATELY and never reaches that error path —
    the error path's own `print` would fail identically on the same closed
    pipe, doubling the fault (#1093 review round 4, finding 8).
    """
    del db
    try:
        typed_args = msgspec.convert(vars(args), type=_AuditRetagDivergenceArgs)
        report = scan_retag_divergence_from_factory(
            lambda: _open_beets(
                typed_args.beets_db,
                typed_args.beets_directory,
            ),
            after_album_id=typed_args.after_album_id,
        )
        if typed_args.json:
            print(json.dumps(msgspec.to_builtins(report), indent=2))
        else:
            _render_retag_divergence_text(report)
    except BrokenPipeError:
        # A downstream reader (e.g. `| head`) closed its end early. Not a
        # program defect — never attempt the error-JSON print below, which
        # would fail on the same closed pipe.
        return 0
    except Exception as exc:  # noqa: BLE001 - transport boundary, not typed B
        print(json.dumps({
            "error": "retag_divergence_audit_failed",
            "detail": str(exc),
        }))
        return 5
    if report.status == "divergence_found":
        return 1
    if report.status == "incomplete":
        return 4
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
    A `BrokenPipeError` from a downstream reader (e.g. `| head`) closing
    early is caught SEPARATELY and never reaches the error path below —
    that path's own `print` would fail identically on the same closed
    pipe, doubling the fault instead of exiting quietly (#1093 review
    round 4, finding 8: moving the render inside the try for the fix above
    made a plain uncaught `BrokenPipeError` — benign on its own — get
    caught by the broad `except Exception`, which then tried to print the
    error JSON and raised a SECOND `BrokenPipeError` that nothing caught).
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
    except BrokenPipeError:
        return 0
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
    retag_divergence.add_argument(
        "--after-album-id",
        type=int,
        default=None,
        help=(
            "Resume a previously truncated scan after this album id "
            "(see next_after_album_id in a prior --json report)."
        ),
    )


__all__ = [
    "add_audit_subparser",
    "cmd_audit_retag_divergence",
    "cmd_audit_world",
]
