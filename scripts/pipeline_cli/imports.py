"""pipeline-cli import commands (#495 carve).

``force-import`` / ``import-jobs`` / ``import-job-recovery`` / ``import-preview``
— the import-queue operator surface: force a rejected download through,
list recent queue jobs, and preview
whether an import would pass without actually running one.
"""

import argparse
import json
import os
import sys

import msgspec

from lib.import_preview import ImportPreviewValues
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    force_import_dedupe_key,
    force_import_payload,
)
from lib.util import (
    resolve_failed_path as _shared_resolve_failed_path,
)
from scripts.pipeline_cli.quality import _load_runtime_rank_config

SPECTRAL_GRADE_CHOICES = ("genuine", "marginal", "suspect", "likely_transcode")

# Known slskd download dirs to resolve old relative failed_paths against
SLSKD_DOWNLOAD_DIRS = ["/mnt/virtio/music/slskd"]


def _resolve_failed_path(failed_path: str) -> "str | None":
    """Resolve a failed_path to an existing absolute directory.

    Old entries stored relative paths (e.g. 'failed_imports/Foo - Bar').
    New entries store absolute paths. Try the path as-is first, then
    resolve against known slskd download dirs.
    """
    return _shared_resolve_failed_path(
        failed_path,
        search_dirs=SLSKD_DOWNLOAD_DIRS,
    )


def cmd_force_import(db, args):
    """Force-import a rejected download by download_log ID."""
    log_id = args.download_log_id

    # 1. Look up download_log entry
    entry = db.get_download_log_entry(log_id)
    if not entry:
        print(f"  Download log entry {log_id} not found.")
        return

    request_id = entry["request_id"]

    # 2. Extract failed_path from validation_result JSONB
    vr_raw = entry.get("validation_result")
    if not vr_raw:
        print(f"  No validation_result on download_log {log_id}.")
        return

    from lib.validation_envelope import decode_validation_envelope

    failed_path = decode_validation_envelope(vr_raw).failed_path
    if not failed_path:
        print(f"  No failed_path in validation_result for download_log {log_id}.")
        return

    # 3. Look up album_request for MBID
    req = db.get_request(request_id)
    if not req:
        print(f"  Album request {request_id} not found.")
        return

    mbid = req["mb_release_id"]
    if not mbid:
        print(f"  Album request {request_id} has no mb_release_id (Discogs-only?).")
        return

    # 4. Resolve and verify files exist
    resolved_path = _resolve_failed_path(failed_path)
    if not resolved_path:
        print(f"  Files not found at: {failed_path}")
        if not os.path.isabs(failed_path):
            print(f"  (also tried: {', '.join(os.path.join(b, failed_path) for b in SLSKD_DOWNLOAD_DIRS)})")
        return
    failed_path = resolved_path

    print(f"  Force-importing: {req['artist_name']} - {req['album_title']}")
    print(f"  Path: {failed_path}")
    print(f"  MBID: {mbid}")

    job = db.enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=request_id,
        dedupe_key=force_import_dedupe_key(log_id),
        payload=force_import_payload(
            download_log_id=log_id,
            failed_path=failed_path,
            source_username=entry.get("soulseek_username"),
        ),
        message=f"Force import queued for {req['artist_name']} - {req['album_title']}",
    )
    deduped = " existing" if job.deduped else ""
    print(f"  [OK] Queued{deduped} import job #{job.id} ({job.status}).")


def cmd_import_jobs(db, args):
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


def cmd_import_job_recovery(db, args) -> int:
    """Resolve one ambiguous Beets operation by explicit operator choice."""
    from lib.import_job_recovery_service import resolve_import_job_recovery

    try:
        result = resolve_import_job_recovery(
            db,
            args.job_id,
            resolution=args.resolution,
            reason=args.reason,
        )
    except ValueError as exc:
        print(f"  {exc}", file=sys.stderr)
        return 2
    if result.outcome == "not_found":
        print(f"  {result.message}", file=sys.stderr)
        return 3
    if result.outcome in ("wrong_state", "authority_changed"):
        print(f"  {result.message}", file=sys.stderr)
        return 4
    print(f"  [OK] {result.message}")
    return 0


def _preview_values_from_args(args) -> ImportPreviewValues:
    raw: dict[str, object] = {}
    if args.values_json:
        parsed = json.loads(args.values_json)
        if not isinstance(parsed, dict):
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


def _print_preview_result(result, *, json_output: bool) -> None:
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


def cmd_import_preview(db, args):
    """Preview a real folder/download-log row or a typed values scenario."""
    from lib.import_preview import (
        preview_import_from_download_log,
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
        if args.download_log_id is not None:
            result = preview_import_from_download_log(db, args.download_log_id)
        elif args.path is not None:
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


def add_imports_subparsers(sub: argparse._SubParsersAction) -> None:
    """Add ``force-import`` / ``import-jobs`` / ``import-job-recovery`` /
    ``import-preview`` (#521 carve out of ``routes_meta._build_parser``,
    verbatim argument definitions)."""
    # force-import
    p_force = sub.add_parser("force-import", help="Force-import a rejected download by download_log ID")
    p_force.add_argument("download_log_id", type=int, help="Download log ID")
    p_force.add_argument("--verified-lossless-target",
                         help="Override the runtime verified-lossless target for this import")

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
        help="Resolve a recovery-required Beets import operation",
    )
    p_recovery.add_argument("job_id", type=int, help="Recovery import job ID")
    p_recovery.add_argument(
        "--resolution",
        required=True,
        choices=["retry", "close"],
        help=(
            "retry only after confirming Beets did not apply; close after "
            "manual reconciliation without replay"
        ),
    )
    p_recovery.add_argument(
        "--reason",
        required=True,
        help="Operator audit reason for the resolution",
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
