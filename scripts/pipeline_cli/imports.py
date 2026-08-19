"""pipeline-cli import commands (#495 carve).

``force-import`` / ``import-local`` / ``import-jobs`` / ``import-job-recovery``
/ ``import-preview`` — the import-queue operator surface: force a rejected
download through, import a folder already on disk against a request's exact
release (issue #1176 PR3), list recent queue jobs, and preview whether an
import would pass without actually running one.

``force-import``, ``import-local``, and ``import-preview --download-log-id``
all open a DB-owned/configured path under a private ``0700`` tree, so all
three execute through their canonical web routes over the permissioned
Unix socket (issue #1063). ``import-preview``'s other two modes stay
in-process on purpose and neither is a fallback for a routed one:
``--values`` is a pure decision that touches no filesystem, and
``--path`` is the explicit-path inspector that CD-SEC-03 deliberately
keeps off the HTTP surface (``docs/security-audit-2026-07-12.md``,
``docs/webui-primer.md``). Each mode has exactly one execution path.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import TYPE_CHECKING, Protocol

import msgspec

from lib.beets_db import BeetsDB, open_beets_db
from lib.import_preview import ImportPreviewValues
from lib.import_queue import ImportJob
from lib.json_narrow import is_object_list, is_str_object_dict
from scripts.pipeline_cli.api_mutations import (
    TIMEOUT_ENQUEUE_SECONDS,
    TIMEOUT_MEASUREMENT_SECONDS,
    _ApiMutation,
    relay_rendered,
    render_api_error,
)
from scripts.pipeline_cli.quality import _load_runtime_rank_config

if TYPE_CHECKING:
    from lib.import_job_recovery_service import (
        AutomationRecoveryDetailDB,
    )
    from lib.import_preview import ImportPreviewDB, ImportPreviewResult

SPECTRAL_GRADE_CHOICES = ("genuine", "marginal", "suspect", "likely_transcode")


class _ImportJobsDB(Protocol):
    """``db`` shape ``cmd_import_jobs`` touches (issue #784, #409 pattern)."""

    def list_import_jobs(
        self, *, status: str | None = None, limit: int = 50,
    ) -> list[ImportJob]: ...


def _open_recovery_beets(
    path: str | None,
    library_root: str | None,
) -> BeetsDB:
    return open_beets_db(db_path=path, library_root=library_root)


def _render_force_import(status: int, payload: dict[str, object]) -> None:
    if 200 <= status < 300:
        job = payload.get("job")
        job_status = job.get("status") if is_str_object_dict(job) else None
        deduped = " existing" if payload.get("deduped") else ""
        print(
            f"  [OK] Queued{deduped} import job "
            f"#{payload.get('job_id')} ({job_status})."
        )
        return
    if payload.get("processing_owner") is not None:
        # The exact owner blob stays machine-readable, as it was when this
        # command enqueued in-process.
        print(json.dumps(payload, sort_keys=True))
        return
    error = payload.get("error") or payload.get("detail")
    print(f"  Force import rejected: {error or status}.")


def cmd_force_import(_db: object, args: argparse.Namespace) -> int:
    """Force-import a rejected download by download_log ID.

    Thin adapter over ``POST /api/pipeline/force-import``, the one
    execution path for both surfaces. The route's authority preflight
    opens the quarantine folder as the service identity, which is the
    only identity that can (issue #1063).

    Exit codes, derived from ``FORCE_IMPORT_HTTP_STATUS``:
      * 0 — 202 queued
      * 2 — 404 download log / request missing
      * 3 — 422 missing release id, missing failed_path, unauthorized path
      * 4 — 409 processing-locked
      * 5 — 503 ``path_unavailable`` (the quarantine authority could not
            OBSERVE the folder — permissions, I/O; retryable, and never
            a claim that the path is wrong or gone)
    """
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path="/api/pipeline/force-import",
            body={"download_log_id": int(args.download_log_id)},
        ),
        render=_render_force_import,
        json_output=False,
        timeout_seconds=TIMEOUT_ENQUEUE_SECONDS,
    )


def _render_import_local(status: int, payload: dict[str, object]) -> None:
    if 200 <= status < 300:
        job = payload.get("job")
        job_status = job.get("status") if is_str_object_dict(job) else None
        deduped = " existing" if payload.get("deduped") else ""
        print(
            f"  [OK] Queued{deduped} import job "
            f"#{payload.get('job_id')} ({job_status})."
        )
        return
    if payload.get("processing_owner") is not None:
        print(json.dumps(payload, sort_keys=True))
        return
    error = payload.get("error") or payload.get("detail")
    print(f"  Local import rejected: {error or status}.")


def cmd_import_local(_db: object, args: argparse.Namespace) -> int:
    """Import a folder already on disk against a request's exact release.

    Thin adapter over ``POST /api/pipeline/import-local`` (issue #1176
    PR3), the one execution path for both surfaces — mirrors
    ``cmd_force_import``'s shape. The route's authority preflight opens the
    configured local-import root as the service identity, which is the
    only identity that can.

    Exit codes, derived from ``LOCAL_IMPORT_HTTP_STATUS``:
      * 0 — 202 queued
      * 2 — 404 request missing
      * 3 — 422 missing release id, local-import lane not configured, or
            unauthorized path (outside the configured root, or inside a
            Cratedigger-owned subtree)
      * 4 — 409 processing-locked
      * 5 — 503 ``path_unavailable`` (the local-import authority could not
            OBSERVE the folder — permissions, I/O; retryable, and never a
            claim that the path is wrong or gone)
    """
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path="/api/pipeline/import-local",
            body={
                "request_id": int(args.request_id),
                "source_path": str(args.source_path),
            },
        ),
        render=_render_import_local,
        json_output=False,
        timeout_seconds=TIMEOUT_ENQUEUE_SECONDS,
    )


def cmd_import_jobs(db: _ImportJobsDB, args: argparse.Namespace) -> None:
    """List recent import queue jobs."""
    jobs = db.list_import_jobs(status=args.status, limit=args.limit)
    if not jobs:
        print("  No import jobs found.")
        return
    for job in jobs:
        request = f"request={job.request_id}" if job.request_id is not None else "request=-"
        msg = job.message or job.error or ""
        print(
            f"  [{job.id:4d}] {job.status:9s} {job.job_type:17s} "
            f"{request:12s} attempts={job.attempts} {msg}"
        )
        if job.status == "recovery_required":
            print(
                "       launch: "
                f"release={job.beets_launch_release_id or '-'} "
                f"source={job.beets_launch_source_path or '-'} "
                f"snapshot={job.beets_launch_snapshot_fingerprint or '-'} "
                f"authorized={job.beets_launch_authorized_at or '-'}"
            )


def cmd_import_job_recovery(
    db: AutomationRecoveryDetailDB, args: argparse.Namespace,
) -> int:
    """Show diagnostics for one ambiguous Beets operation."""
    from lib.import_job_recovery_service import (
        get_automation_recovery_detail,
    )

    beets: BeetsDB | None = None
    try:
        beets = _open_recovery_beets(
            getattr(args, "beets_db", None),
            getattr(args, "beets_directory", None),
        )
    except Exception:  # noqa: BLE001 - unavailable is a typed observation
        beets = None
    if beets is None:
        detail_result = get_automation_recovery_detail(
            db,
            None,
            args.job_id,
        )
    else:
        with beets:
            detail_result = get_automation_recovery_detail(
                db,
                beets,
                args.job_id,
            )
    print(json.dumps(detail_result.to_dict(), indent=2, sort_keys=True))
    return 0 if detail_result.outcome == "ok" else 2


def _preview_values_from_args(args: argparse.Namespace) -> ImportPreviewValues:
    raw: dict[str, object] = {}
    if args.values_json:
        parsed: object = json.loads(args.values_json)
        if not is_str_object_dict(parsed):
            raise ValueError("--values-json must be a JSON object")
        raw.update(parsed)

    for attr in (
        "is_flac",
        "min_bitrate",
        "is_cbr",
        "is_vbr",
        "avg_bitrate",
        "spectral_grade",
        "spectral_bitrate",
        "existing_min_bitrate",
        "existing_avg_bitrate",
        "existing_spectral_bitrate",
        "existing_spectral_grade",
        "override_min_bitrate",
        "existing_format",
        "existing_is_cbr",
        "post_conversion_min_bitrate",
        "converted_count",
        "verified_lossless",
        "verified_lossless_target",
        "target_format",
        "new_format",
        "audio_check_mode",
        "audio_corrupt",
        "has_nested_audio",
    ):
        value = getattr(args, attr, None)
        if value is not None:
            raw[attr] = value
    for attr in ("spectral_grade", "existing_spectral_grade"):
        value = raw.get(attr)
        if value is not None and value not in SPECTRAL_GRADE_CHOICES:
            valid = ", ".join(SPECTRAL_GRADE_CHOICES)
            raise ValueError(f"{attr} must be one of: {valid}")
    return msgspec.convert(raw, type=ImportPreviewValues)


def _print_preview_result(
    result: ImportPreviewResult, *, json_output: bool,
) -> None:
    if json_output:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return
    print(f"  verdict: {result.verdict}")
    if result.decision:
        print(f"  decision: {result.decision}")
    if result.reason and result.reason != result.decision:
        print(f"  reason: {result.reason}")
    if result.detail:
        print(f"  detail: {result.detail}")
    if result.cleanup_eligible:
        print("  cleanup_eligible: yes")
    if result.stage_chain:
        print("  stages:")
        for stage in result.stage_chain:
            print(f"    - {stage}")


def import_preview_is_routed(args: argparse.Namespace) -> bool:
    """Does this invocation touch a DB-owned protected quarantine path?

    Only ``--download-log-id`` does. ``cli.py`` asks this before it
    builds a database handle, so the routed mode never acquires one.
    """
    return getattr(args, "download_log_id", None) is not None


def _render_import_preview(status: int, payload: dict[str, object]) -> None:
    if payload.get("verdict") is None:
        render_api_error(status, payload)
        return
    print(f"  verdict: {payload.get('verdict')}")
    decision = payload.get("decision")
    if decision:
        print(f"  decision: {decision}")
    reason = payload.get("reason")
    if reason and reason != decision:
        print(f"  reason: {reason}")
    if payload.get("detail"):
        print(f"  detail: {payload['detail']}")
    if payload.get("cleanup_eligible"):
        print("  cleanup_eligible: yes")
    stages = payload.get("stage_chain")
    if is_object_list(stages) and stages:
        print("  stages:")
        for stage in stages:
            print(f"    - {stage}")


def cmd_import_preview_from_download_log(
    _db: object, args: argparse.Namespace,
) -> int:
    """Preview one download_log row through the canonical web route.

    The row's ``failed_path`` lives under the private processing tree,
    and the route's snapshot runs as the service identity (issue #1063).

    Exit codes: 0 on 200, 3 on 400/422 (bad id), 5 otherwise.
    """
    if args.path is not None or args.values or args.values_json is not None:
        print(
            "  Provide exactly one mode: --download-log-id, --request-id/--path, or --values.",
            file=sys.stderr,
        )
        return 2
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path="/api/import-preview",
            body={"download_log_id": int(args.download_log_id)},
        ),
        render=_render_import_preview,
        json_output=args.json,
        timeout_seconds=TIMEOUT_MEASUREMENT_SECONDS,
    )


def cmd_import_preview(
    db: ImportPreviewDB, args: argparse.Namespace,
) -> int:
    """Preview an explicit folder or a typed values scenario.

    The ``--download-log-id`` mode is not handled here — ``cli.py``
    routes it to :func:`cmd_import_preview_from_download_log` before any
    database handle exists.
    """
    from lib.import_preview import (
        preview_import_from_path,
        preview_import_from_values,
    )

    mode_count = sum(bool(v) for v in (
        args.download_log_id is not None,
        args.path is not None,
        args.values or args.values_json is not None,
    ))
    if mode_count != 1:
        print(
            "  Provide exactly one mode: --download-log-id, --request-id/--path, or --values.",
            file=sys.stderr,
        )
        return 2

    try:
        if args.path is not None:
            if args.request_id is None:
                print("  --request-id is required with --path", file=sys.stderr)
                return 2
            result = preview_import_from_path(
                db,
                request_id=args.request_id,
                path=args.path,
                force=not args.no_force,
            )
        else:
            result = preview_import_from_values(
                _preview_values_from_args(args),
                cfg=_load_runtime_rank_config(),
            )
    except (ValueError, TypeError, msgspec.ValidationError) as exc:
        print(f"  Invalid preview input: {exc}", file=sys.stderr)
        return 2

    _print_preview_result(result, json_output=args.json)
    return 0


def add_imports_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``force-import`` / ``import-local`` / ``import-jobs`` /
    ``import-job-recovery`` / ``import-preview`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # force-import
    p_force = sub.add_parser("force-import", help="Force-import a rejected download by download_log ID")
    p_force.add_argument("download_log_id", type=int, help="Download log ID")
    p_force.add_argument("--verified-lossless-target",
                         help="Override the runtime verified-lossless target for this import")

    # import-local (issue #1176 PR3)
    p_local = sub.add_parser(
        "import-local",
        help="Import a folder already on disk against a request's exact release",
    )
    p_local.add_argument("request_id", type=int, help="Album request ID")
    p_local.add_argument(
        "source_path",
        help="Absolute path under the configured local-import root",
    )

    # import-jobs
    p_jobs = sub.add_parser("import-jobs", help="List recent import queue jobs")
    p_jobs.add_argument(
        "--status",
        choices=[
            "queued",
            "running",
            "recovery_required",
            "completed",
            "failed",
        ],
    )
    p_jobs.add_argument("--limit", type=int, default=20)

    p_recovery = sub.add_parser(
        "import-job-recovery",
        help="Inspect diagnostic evidence for one Beets import operation",
    )
    recovery_actions = p_recovery.add_subparsers(
        dest="recovery_action",
        required=True,
    )
    p_recovery_show = recovery_actions.add_parser(
        "show",
        help="Show automation recovery diagnostics",
    )
    p_recovery_show.add_argument(
        "job_id",
        type=int,
        help="Recovery import job ID",
    )
    p_recovery_show.add_argument(
        "--beets-db",
        default=None,
        help="Explicit Beets SQLite override; requires --beets-directory",
    )
    p_recovery_show.add_argument(
        "--beets-directory",
        default=None,
        help="Library root paired with --beets-db",
    )
    # import-preview
    p_preview = sub.add_parser("import-preview", help="Preview whether an import would pass")
    p_preview.add_argument("--download-log-id", type=int,
                           help="Preview the failed_path from a download_log row")
    p_preview.add_argument("--request-id", type=int,
                           help="Request ID for --path preview")
    p_preview.add_argument("--path", help="Preview a real folder for a request")
    p_preview.add_argument("--no-force", action="store_true",
                           help="Do not pass --force to import_one.py preview")
    p_preview.add_argument("--values", action="store_true",
                           help="Preview typed override values instead of a real folder")
    p_preview.add_argument("--values-json",
                           help="JSON object with ImportPreviewValues fields")
    p_preview.add_argument("--json", action="store_true",
                           help="Print the common preview result as JSON")
    p_preview.add_argument("--is-flac", action="store_true", default=None)
    p_preview.add_argument("--min-bitrate", type=int)
    p_preview.add_argument("--is-cbr", action="store_true", default=None)
    p_preview.add_argument("--is-vbr", action="store_true", default=None)
    p_preview.add_argument("--avg-bitrate", type=int)
    p_preview.add_argument("--spectral-grade", choices=SPECTRAL_GRADE_CHOICES)
    p_preview.add_argument("--spectral-bitrate", type=int)
    p_preview.add_argument("--existing-min-bitrate", type=int)
    p_preview.add_argument("--existing-avg-bitrate", type=int)
    p_preview.add_argument("--existing-spectral-bitrate", type=int)
    p_preview.add_argument("--existing-spectral-grade", choices=SPECTRAL_GRADE_CHOICES)
    p_preview.add_argument("--override-min-bitrate", type=int)
    p_preview.add_argument("--existing-format")
    p_preview.add_argument("--existing-is-cbr", action="store_true", default=None)
    p_preview.add_argument("--post-conversion-min-bitrate", type=int)
    p_preview.add_argument("--converted-count", type=int)
    p_preview.add_argument("--verified-lossless", action="store_true", default=None)
    p_preview.add_argument("--verified-lossless-target")
    p_preview.add_argument("--target-format")
    p_preview.add_argument("--new-format")
    p_preview.add_argument("--audio-check-mode")
    p_preview.add_argument("--audio-corrupt", action="store_true", default=None)
    p_preview.add_argument("--has-nested-audio", action="store_true", default=None)
