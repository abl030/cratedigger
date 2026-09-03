"""Read-only cross-engine audit commands."""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import TYPE_CHECKING

import msgspec

from lib.beets_db import BeetsDB, open_beets_db
from lib.retag_divergence_audit import (
    RetagDivergenceReport,
    SingleAlbumRetagCheckResult,
    is_valid_album_id,
    parse_after_album_id_cursor,
    scan_retag_divergence_from_factory,
    scan_retag_divergence_single_album_from_factory,
)
from lib.world_audit_service import (
    WORLD_AUDIT_EXIT_CODES,
    WorldAuditReport,
    audit_world_from_factory,
    world_audit_outcome,
)

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


class _AuditRetagDivergenceAlbumArgs(msgspec.Struct, frozen=True):
    album_id: int
    beets_db: str | None = None
    beets_directory: str | None = None
    json: bool = False


def _open_beets(path: str | None, library_root: str | None) -> BeetsDB:
    return open_beets_db(db_path=path, library_root=library_root)


def _handle_broken_pipe_and_exit_cleanly() -> int:
    """A downstream reader (e.g. `| head`) closed its end early.

    stdout to a pipe is BLOCK-buffered (not line-buffered like a TTY), so a
    `print()` alone often does not raise even after the reader has already
    closed — the write only reaches the kernel, and the closed-read-end
    check, at the NEXT flush. Left unhandled, that flush usually happens at
    Python's own interpreter-shutdown cleanup — AFTER this function has
    already returned and outside every `except` clause here — which
    overrides this function's own return value and exits the whole process
    120 regardless
    (https://docs.python.org/3/library/signal.html#note-on-sigpipe). Both
    callers force that flush explicitly, inside their own `try`, so THIS
    handler is what actually observes and handles the pipe closing. Once
    caught, redirect stdout's underlying file descriptor to `/dev/null` so
    the unavoidable final interpreter flush becomes a no-op instead of
    raising a second, uncaught `BrokenPipeError` (#1093 review round 5,
    finding 4 — the previous handler assumed a real pipe surfaces this
    exception on every write the way a synthetic always-raising test double
    does, which no real pipe does; verified end-to-end against a real OS
    pipe with a real early-closing reader in
    `tests/test_pipeline_cli.py::TestRealBrokenPipeHandling`). If `sys.
    stdout` isn't backed by a real OS file descriptor at all (e.g. a test
    double substituted via `redirect_stdout`, never real production
    stdout) there is nothing to redirect — its own eventual flush can't
    trigger the OS-level EPIPE-at-shutdown failure this exists to prevent,
    so skip the redirect rather than raising on the substitute's missing
    `fileno()`.
    """
    try:
        fd = sys.stdout.fileno()
    except (AttributeError, OSError, ValueError):
        return 0
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, fd)
    os.close(devnull)
    return 0


def _argparse_after_album_id(text: str) -> int:
    """``--after-album-id`` type callable — shares the strict grammar the
    API's ``?after_album_id=`` uses (``lib.retag_divergence_audit.
    parse_after_album_id_cursor``), so a value argparse's bare ``int()``
    would silently reinterpret (underscore grouping, a leading sign,
    surrounding whitespace, non-ASCII digits) is refused on the CLI exactly
    as it is over HTTP — CLI ⇄ API Surface Symmetry (#1093 review round 5,
    finding 5)."""
    try:
        return parse_after_album_id_cursor(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _argparse_album_id(text: str) -> int:
    """``retag-divergence-album``'s positional ``album_id`` type callable
    — rejects an id past SQLite's signed-64-bit ``INTEGER`` range at the
    CLI's own input boundary, mirroring the HTTP route's 400 (#1142
    review N10, CLI ⇄ API Surface Symmetry): such an id can never be
    bound as a query parameter (``sqlite3`` raises ``OverflowError``
    before any query runs), so it is rejected before ever reaching
    Beets rather than surfacing as a confusing exit 5."""
    try:
        album_id = parse_after_album_id_cursor(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc
    if not is_valid_album_id(album_id):
        raise argparse.ArgumentTypeError(f"album id {album_id} is out of range")
    return album_id


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
    (unreadable/empty/refused-only findings, a truncated deadline, OR a
    ``--after-album-id`` cursor was given — a resumed call never claims
    ``clean`` on its own, even when it completes, since it only vouches for
    the range it actually scanned; #1093 review round 5, finding 1); this
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
    the cohort really is empty. `cmd_audit_world` used to exit 0 for its
    own analogous beets-unavailable bucket — a pre-existing deviation from
    this same documented convention, closed by issue #1355 item 4: it now
    exits 5 for `beets_unavailable` too, matching this command exactly.

    Exit 5 also covers an unexpected transport/decode/render defect — the
    whole body below runs inside the try so a
    `msgspec.convert`/serialization failure can't traceback out as an
    unmapped exit 1 (#1093 review round 2, finding 5; the previous shape
    left the argument decode and the render/json-encode steps outside the
    try). An explicit `sys.stdout.flush()` right after the render forces a
    downstream reader closing early (e.g. `| head`) to surface as a
    `BrokenPipeError` HERE, inside this try, rather than at Python's own
    interpreter-shutdown flush after this function has already returned —
    which would exit the whole process 120 regardless of any return value
    (#1093 review round 5, finding 4: stdout is block-buffered on a real
    pipe, so a bare `print()` without an explicit flush often does not
    raise even after the reader has closed). That `BrokenPipeError` is
    caught SEPARATELY via `_handle_broken_pipe_and_exit_cleanly` and never
    reaches the error path below — that path's own `print` would fail
    identically on the same closed pipe, doubling the fault (#1093 review
    round 4, finding 8).
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
        # Force the write through now, inside this try, rather than
        # leaving it to Python's own interpreter-shutdown flush — see
        # `_handle_broken_pipe_and_exit_cleanly`.
        sys.stdout.flush()
    except BrokenPipeError:
        return _handle_broken_pipe_and_exit_cleanly()
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


def _render_retag_divergence_album_text(result: SingleAlbumRetagCheckResult) -> None:
    print(f"retag divergence album check: {result.status}")
    if result.unavailable_detail:
        print(f"unavailable: {result.unavailable_detail}")
    album = result.album
    if album is None:
        return
    print(
        f"album {album.album_id} ({album.album_class}): "
        f"db_mb_albumid={album.db_mb_albumid!r} item_count={album.item_count}"
    )
    for item in album.items:
        detail = f" — {item.detail}" if item.detail else ""
        print(
            f"  {item.item_class}: {item.path} "
            f"(file_mb_albumid={item.file_mb_albumid!r}){detail}"
        )


def cmd_audit_retag_divergence_album(db: object, args: object) -> int:
    """Cheap, explicit per-album retag-divergence recheck (#1142) — the
    CLI counterpart of ``GET /api/audit/retag-divergence/album/<id>``.
    Reuses the SAME classifier/tag-reader as the whole-library census
    (``scan_retag_divergence_single_album_from_factory``), over exactly
    one album — never the whole-library scan.

    Read-only over Beets alone — ``db`` (the pipeline DB connection every
    ``audit`` subcommand's dispatch dict is called with) is unused.

    Exit-code mapping mirrors the HTTP route's status-code mapping (CLI
    ⇄ API Surface Symmetry): 0 for ``found`` (any album class, including
    an agreeing album — this is an explicit lookup, not a health-check
    gate, so the exit code reflects whether the check ANSWERED, not
    whether it found a problem), 2 for ``not_found`` (matching the
    route's 404), 5 for ``beets_unavailable`` (matching the route's 503)
    or an unexpected transport/decode/render defect — the whole body
    runs inside the try, mirroring ``cmd_audit_retag_divergence``'s own
    shape.
    """
    del db
    try:
        typed_args = msgspec.convert(
            vars(args), type=_AuditRetagDivergenceAlbumArgs,
        )
        result = scan_retag_divergence_single_album_from_factory(
            lambda: _open_beets(
                typed_args.beets_db, typed_args.beets_directory,
            ),
            typed_args.album_id,
        )
        if typed_args.json:
            print(json.dumps(msgspec.to_builtins(result), indent=2))
        else:
            _render_retag_divergence_album_text(result)
        sys.stdout.flush()
    except BrokenPipeError:
        return _handle_broken_pipe_and_exit_cleanly()
    except Exception as exc:  # noqa: BLE001 - transport boundary, not typed B
        print(json.dumps({
            "error": "retag_divergence_album_check_failed",
            "detail": str(exc),
        }))
        return 5
    if result.status == "not_found":
        return 2
    if result.status == "beets_unavailable":
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
    An explicit `sys.stdout.flush()` right after the render forces a
    downstream reader closing early (e.g. `| head`) to surface as a
    `BrokenPipeError` HERE rather than at Python's own interpreter-shutdown
    flush after this function has already returned, which would exit the
    whole process 120 regardless of any return value (#1093 review round
    5, finding 4 — stdout is block-buffered on a real pipe, so a bare
    `print()` alone often does not raise even after the reader has
    closed). That `BrokenPipeError` is caught SEPARATELY via
    `_handle_broken_pipe_and_exit_cleanly` and never reaches the error path
    below — that path's own `print` would fail identically on the same
    closed pipe, doubling the fault instead of exiting quietly (#1093
    review round 4, finding 8: moving the render inside the try for the
    fix above made a plain uncaught `BrokenPipeError` — benign on its own
    — get caught by the broad `except Exception`, which then tried to
    print the error JSON and raised a SECOND `BrokenPipeError` that
    nothing caught).

    Exit-code mapping (issue #1355 item 4), derived via
    `lib.world_audit_service.world_audit_outcome`: `clean`/
    `observations_only` -> `0` (a COMPLETE report, whatever it observed —
    Bucket B/C findings stay visible without failing the command); `1` iff
    `status == "integrity_failed"` (a genuine Bucket A finding — the one
    thing this instrument exists to surface, mirroring the sibling retag-
    divergence audit's own `divergence_found` -> `1`, both deliberately
    outside the ordinary CLI ⇄ API Surface Symmetry convention table);
    `beets_unavailable` (`complete == False`) -> `5` (transient/retryable
    — the audit never actually ran, so exit `0` there would let a cron or
    `&&` chain read "clean" from a report that answered nothing). This
    used to exit `0` for `beets_unavailable` too — a pre-existing
    deviation from `cmd_audit_retag_divergence`'s own convention, closed
    by #1355 item 4; the current test pinning `beets_unavailable` now
    expects exit `5`, matching the sibling exactly.
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
        # Force the write through now, inside this try — see
        # `_handle_broken_pipe_and_exit_cleanly`.
        sys.stdout.flush()
    except BrokenPipeError:
        return _handle_broken_pipe_and_exit_cleanly()
    except Exception as exc:  # noqa: BLE001 - transport boundary, not typed B
        print(json.dumps({
            "error": "world_audit_failed",
            "detail": str(exc),
        }))
        return 5
    outcome = world_audit_outcome(report)
    if outcome == "integrity_failed":
        return 1
    return WORLD_AUDIT_EXIT_CODES[outcome]


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
        type=_argparse_after_album_id,
        default=None,
        help=(
            "Resume a previously truncated scan after this album id "
            "(see next_after_album_id in a prior --json report)."
        ),
    )
    retag_divergence_album = operations.add_parser(
        "retag-divergence-album",
        help=(
            "Cheap, explicit per-album retag-divergence recheck — the "
            "same classifier as retag-divergence, over one album's own "
            "files only (#1142)."
        ),
    )
    retag_divergence_album.add_argument(
        "album_id", type=_argparse_album_id, help="Beets album id to recheck.",
    )
    _add_beets_override_args(retag_divergence_album)


__all__ = [
    "add_audit_subparser",
    "cmd_audit_retag_divergence",
    "cmd_audit_retag_divergence_album",
    "cmd_audit_world",
]
