#!/usr/bin/env python3
"""Pipeline CLI — manage the download pipeline database.

Commands:
    list [status]       List album requests (optionally filtered by status)
    add <mbid>          Add a new request by MusicBrainz release ID
    query <sql>         Run a read-only SQL query for debugging
    status              Show counts by status
    retry <id>          Reset a failed/rejected request to wanted
    cancel <id>         Set a request to skipped
    set <id> <status>   Change status (wanted, imported, manual)
    show <id>           Show full details of a request
    force-import <dl_id> Force-import a rejected download by download_log ID
    manual-import <id> <path> Import a local folder as a pipeline request

Usage:
    python3 scripts/pipeline_cli.py status
    python3 scripts/pipeline_cli.py list wanted
    python3 scripts/pipeline_cli.py add 44438bf9-26d9-4460-9b4f-1a1b015e37a1 --source request
    python3 scripts/pipeline_cli.py retry 42
    python3 scripts/pipeline_cli.py migrate --dry-run
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, time
from decimal import Decimal

# Surface INFO-level log lines (e.g. the [import] stderr passthrough from
# dispatch_import_core) so force-import / manual-import failures are visible to
# the user instead of silently swallowed by Python's default WARNING-only
# logger configuration.
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    stream=sys.stderr,
)

import msgspec
import psycopg2

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from lib import transitions

# Module-level DI seam for ``transitions.finalize_request`` — see
# ``lib.import_dispatch.finalize_request`` for the rationale.
finalize_request = transitions.finalize_request

from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_MANUAL,
    force_import_dedupe_key,
    force_import_payload,
    manual_import_dedupe_key,
    manual_import_payload,
)
from lib.pipeline_db import PipelineDB, DEFAULT_DSN
from lib.import_preview import ImportPreviewValues
from lib.disk_coverage_service import disk_coverage
from lib.release_identity import detect_release_source, normalize_release_id
from lib.util import (
    resolve_failed_path as _shared_resolve_failed_path,
)
# CLI ⇄ API symmetry: import the service entrypoint + outcome → exit-code
# mapping directly so the test, the CLI, and the U8 route share one
# source of truth (PR #381 lesson). Do NOT redefine the mapping here.
from lib.youtube_album_service import (
    OUTCOME_EXIT_CODE,
    resolve_youtube_album,
)
# U4 / CLI ⇄ API symmetry: import the YT-rescue ingest service's outcome
# → exit-code mapping with an alias (the youtube_album_service one above
# is already bound). Keep this the single source of truth for the CLI; the
# U5 route imports OUTCOME_HTTP_STATUS from the same module for HTTP-side
# mapping.
from lib.youtube_ingest_service import (
    OUTCOME_EXIT_CODE as YOUTUBE_INGEST_EXIT_CODE,
    default_youtube_ingest_service_factory,
)

MB_API = "http://192.168.1.35:5200/ws/2"
SPECTRAL_GRADE_CHOICES = ("genuine", "marginal", "suspect", "likely_transcode")


def _load_runtime_rank_config():
    """Load the runtime QualityRankConfig from the active config.ini."""
    from lib.config import read_runtime_rank_config

    return read_runtime_rank_config()


def _load_runtime_verified_lossless_target() -> str:
    """Load the runtime verified_lossless_target from the active config.ini."""
    from lib.config import read_verified_lossless_target

    return read_verified_lossless_target()


def _load_runtime_audio_check_mode() -> str:
    """Load the runtime audio_check_mode from the active config.ini.

    Used by the quality simulator so the preimport audio gate scenario
    reflects the deployment's `[Beets Validation] audio_check` setting
    (issue #91). On deployments with `audio_check = off`, the scenario
    shows `skipped_off` instead of `reject_corrupt`.
    """
    from lib.config import read_runtime_config

    return read_runtime_config().audio_check_mode


def _quality_preview_target_label(
    target_format: str | None,
    verified_lossless_target: str | None,
) -> str:
    """Human label for the on-disk destination used in quality previews."""
    if target_format in ("flac", "lossless"):
        return "flac"
    if verified_lossless_target:
        return verified_lossless_target
    return "V0"


def _load_beets_album_info(mb_release_id, rank_cfg):
    """Best-effort Beets album lookup for current quality metadata."""
    from lib.beets_db import BeetsDB

    if not mb_release_id:
        return None
    try:
        with BeetsDB() as beets:
            return beets.get_album_info(mb_release_id, rank_cfg)
    except Exception:
        return None


def fetch_mb_release(mb_release_id):
    """Fetch release metadata + tracks from MusicBrainz API.

    Returns raw MB JSON. The ``media+release-groups+labels`` inc params
    are required so the field resolver service can extract
    ``label-info`` (catalog_number), per-track ``artist-credit``
    (track_artist), and the nested ``release-group`` primary-type/
    secondary-types (VA Rule 2). Without them, every MB request adds
    its rows to the side table as ``unresolved_field_missing_upstream``
    even though MB has the data.
    """
    url = (
        f"{MB_API}/release/{mb_release_id}"
        f"?inc=recordings+artist-credits+media+release-groups+labels&fmt=json"
    )
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "pipeline-cli/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print(f"  [ERROR] MB API: {e}", file=sys.stderr)
        return None


def tracks_from_mb_release(release_data):
    """Extract track list from MB API release response.

    Includes pregap tracks (to match beets' default behaviour) but excludes
    data tracks (beets' ignore_data_tracks defaults to yes).
    """
    tracks = []
    for medium in release_data.get("media", []):
        disc = medium.get("position", 1)
        # Include pregap track if present (beets always counts these)
        if "pregap" in medium:
            pg = medium["pregap"]
            length_ms = pg.get("length") or (pg.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": 0,
                "title": pg.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
        for track in medium.get("tracks", []):
            length_ms = track.get("length") or (track.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": track.get("position", track.get("number", 0)),
                "title": track.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
    return tracks


def cmd_list(db, args):
    if args.filter_status:
        albums = db.get_by_status(args.filter_status)
    else:
        rows = db._execute("SELECT * FROM album_requests ORDER BY created_at ASC").fetchall()
        albums = [dict(r) for r in rows]

    if not albums:
        print("No albums found.")
        return

    for a in albums:
        print(f"  [{a['id']:4d}] {a['status']:12s} {a['source']:10s} "
              f"{a['artist_name']} - {a['album_title']}  "
              f"({a['mb_release_id'] or a.get('discogs_release_id') or 'no-id'})")
    print(f"\n  Total: {len(albums)}")


def cmd_disk_coverage(db, args):
    from lib.beets_db import BeetsDB

    with BeetsDB(args.beets_db) as beets:
        result = disk_coverage(
            db,
            beets,
            include_rows=not args.counts_only,
            include_inverse=args.include_inverse,
        )
    print(json.dumps(msgspec.to_builtins(result), indent=2, sort_keys=True))


def cmd_add(db, args):
    release_id = normalize_release_id(args.mbid)
    source = args.source
    id_source = detect_release_source(release_id)

    if id_source == "discogs":
        return _cmd_add_discogs(db, release_id, source)
    return _cmd_add_mb(db, release_id, source)


def _build_search_plan_service(db):
    """Construct a `SearchPlanService` against the runtime config.

    CLI / web / startup all share the same source so generator-id and
    `SearchPlanConfig` cannot drift between paths.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import SearchPlanService
    return SearchPlanService(db, read_runtime_config())


def _search_plan_exit_code(outcome: str) -> int:
    """CLI ⇄ API exit-code mapping for search-plan read/advance subcommands.

    Per CLAUDE.md § "CLI ⇄ API surface symmetry":
    0=success, 2=not_found, 3=input_validation, 4=wrong_state, 5=transient.

    Covers the outcome strings emitted by ``dry_run_for_request``,
    ``saturation_for_request``, ``advance_for_request`` and
    ``history_for_request``. Regenerate has its own ladder
    (``failed_transient`` → 4 there, predating this convention).
    """
    from lib.search_plan_service import (
        RESULT_ADVANCED,
        RESULT_DRY_RUN_GENERATION_FAILED,
        RESULT_DRY_RUN_SUCCESS,
        RESULT_FAILED_TRANSIENT,
        RESULT_HISTORY_PAGE_INPUT_INVALID,
        RESULT_HISTORY_PAGE_SUCCESS,
        RESULT_INVALID_TARGET,
        RESULT_NO_ACTIVE_PLAN,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_SATURATION_INPUT_INVALID,
        RESULT_SATURATION_SUCCESS,
    )
    mapping: dict[str, int] = {
        RESULT_DRY_RUN_SUCCESS: 0,
        RESULT_DRY_RUN_GENERATION_FAILED: 0,
        RESULT_SATURATION_SUCCESS: 0,
        RESULT_ADVANCED: 0,
        RESULT_HISTORY_PAGE_SUCCESS: 0,
        RESULT_REQUEST_NOT_FOUND: 2,
        RESULT_SATURATION_INPUT_INVALID: 3,
        RESULT_INVALID_TARGET: 3,
        RESULT_HISTORY_PAGE_INPUT_INVALID: 3,
        RESULT_NO_ACTIVE_PLAN: 4,
        RESULT_FAILED_TRANSIENT: 5,
    }
    return mapping.get(outcome, 1)


def _generate_plan_after_add(db, req_id, *, artist_name, album_title, year,
                              tracks, source, release_group_year=None,
                              is_va_compilation=False,
                              catalog_number=None):
    """Run plan generation for a freshly-added request.

    Failures are non-fatal: a deterministic / transient failure is
    recorded as a `search_plans` row and the CLI prints a one-liner so
    the operator knows the request is wanted-but-not-searchable until
    repaired.

    ``release_group_year`` (U5 of search-plan-entropy) feeds the
    generator's conditional ``unwild_rg_year`` slot. ``None`` is fine
    — the generator handles it gracefully.

    PR2 Apply #2: ``is_va_compilation`` and ``catalog_number`` are
    forwarded so the initial plan respects the resolver's verdict —
    mirrors the web add helper for CLI ⇄ API symmetry.
    """
    from lib.search_plan_service import (
        RESULT_FAILED_DETERMINISTIC,
        RESULT_FAILED_TRANSIENT,
        RESULT_SUCCESS,
    )

    svc = _build_search_plan_service(db)
    result = svc.generate_for_new_request(
        req_id,
        artist_name=artist_name,
        album_title=album_title,
        year=year,
        tracks=tracks,
        source=source,
        release_group_year=release_group_year,
        is_va_compilation=is_va_compilation,
        catalog_number=catalog_number,
    )
    if result.outcome == RESULT_SUCCESS:
        print(f"  Plan: active id={result.plan_id}")
    elif result.outcome == RESULT_FAILED_DETERMINISTIC:
        print(f"  Plan: FAILED ({result.failure_class}); request not searchable")
    elif result.outcome == RESULT_FAILED_TRANSIENT:
        print(f"  Plan: TRANSIENT FAIL ({result.failure_class}); will retry")


def _resolve_and_update_after_add(
    db,
    req_id: int,
    *,
    mb_release_id: str | None,
    discogs_release_id: str | None,
    mb_release_group_id: str | None,
    mb_artist_id: str | None,
    mb_release_payload: dict | None = None,
    discogs_release_payload: dict | None = None,
):
    """U4 helper for the CLI add path — mirrors the web helper.

    Both surfaces wrap ``field_resolver_service.resolve_all`` after the
    new request row is inserted, so the CLI and HTTP add stay symmetric
    (CLAUDE.md § "CLI ⇄ API surface symmetry"). The CLI prints a one-
    liner on resolver outcomes so the operator running the script
    knows whether a field landed NULL.
    """
    from lib.field_resolver_service import (
        ResolveAllResult,
        apply_resolve_all_result,
        resolve_all,
    )

    skeleton = {
        "id": req_id,
        "mb_release_id": mb_release_id,
        "discogs_release_id": discogs_release_id,
        "mb_release_group_id": mb_release_group_id,
        "mb_artist_id": mb_artist_id,
    }
    try:
        result = resolve_all(
            skeleton,
            db,
            mb_release_payload=mb_release_payload,
            discogs_release_payload=discogs_release_payload,
        )
    except Exception as exc:  # noqa: BLE001
        print(f"  Field resolution crashed: {exc}", file=sys.stderr)
        return ResolveAllResult()

    try:
        apply_resolve_all_result(
            db, req_id, result,
            existing_mb_release_group_id=mb_release_group_id,
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"  Failed to persist resolved fields: {exc}",
            file=sys.stderr,
        )

    rg_year_str = (
        str(result.release_group_year)
        if result.release_group_year is not None else "NULL"
    )
    va_str = "yes" if result.is_va_compilation else "no"
    timed_out = (
        f" timed_out={','.join(result.timed_out_fields)}"
        if result.timed_out_fields else ""
    )
    print(
        f"  Resolved: rg_year={rg_year_str} is_va={va_str}{timed_out}"
    )
    return result


def _cmd_add_mb(db, mbid, source):
    """Add a MusicBrainz release to the pipeline."""
    existing = db.get_request_by_release_id(mbid)
    if existing:
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return

    print(f"  Fetching MB release {mbid}...")
    release = fetch_mb_release(mbid)
    if not release:
        print("  Failed to fetch release from MB API.")
        return

    artist_credit = release.get("artist-credit", [{}])
    artist_name = artist_credit[0].get("name", "Unknown") if artist_credit else "Unknown"
    artist_id = (artist_credit[0].get("artist", {}).get("id")
                 if artist_credit else None)
    rg_id = (release.get("release-group") or {}).get("id")
    year = None
    if release.get("date"):
        year = int(release["date"][:4]) if len(release["date"]) >= 4 else None

    req_id = db.add_request(
        mb_release_id=mbid,
        mb_release_group_id=rg_id,
        mb_artist_id=artist_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        country=release.get("country"),
        source=source,
    )

    tracks = tracks_from_mb_release(release)
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {artist_name} - {release.get('title')} ({len(tracks)} tracks)")
    # U4: inline field resolution + VA detection. Single resolver-service
    # invocation shared with the web add path (CLI ⇄ API symmetry).
    resolved = _resolve_and_update_after_add(
        db, req_id,
        mb_release_id=mbid,
        discogs_release_id=None,
        mb_release_group_id=rg_id,
        mb_artist_id=artist_id,
        mb_release_payload=release,
    )
    # Re-read tracks from the DB so the per-track ``track_artist``
    # column the resolver just wrote (PR2 Apply #1) flows into the
    # snapshot. The upstream ``tracks`` extracted from the MB payload
    # does NOT carry the resolver's output.
    post_resolve_tracks = db.get_tracks(req_id)
    _generate_plan_after_add(
        db, req_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        tracks=post_resolve_tracks,
        source=source,
        release_group_year=resolved.release_group_year,
        is_va_compilation=resolved.is_va_compilation,
        catalog_number=resolved.catalog_number,
    )


def _cmd_add_discogs(db, discogs_id, source):
    """Add a Discogs release to the pipeline."""
    existing = db.get_request_by_release_id(discogs_id)
    if existing:
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return

    print(f"  Fetching Discogs release {discogs_id}...")
    try:
        from web import discogs as discogs_api
        release = discogs_api.get_release(int(discogs_id))
    except Exception as e:
        print(f"  Failed to fetch release from Discogs API: {e}")
        return

    req_id = db.add_request(
        mb_release_id=discogs_id,
        discogs_release_id=discogs_id,
        mb_artist_id=str(release.get("artist_id") or ""),
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        country=release.get("country"),
        source=source,
    )

    tracks = release.get("tracks", [])
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {release['artist_name']} - {release['title']} ({len(tracks)} tracks)")
    # U4: inline field resolution + VA detection. Discogs add path has
    # no MB release/release-group payload available so the resolver only
    # sees the discogs release (Rule 1 of VA detection still fires on
    # canonical-ID match; rules 2 + 3 are MB-only).
    resolved = _resolve_and_update_after_add(
        db, req_id,
        mb_release_id=None,
        discogs_release_id=discogs_id,
        mb_release_group_id=None,
        mb_artist_id=str(release.get("artist_id") or "") or None,
        discogs_release_payload=release,
    )
    # Re-read tracks from the DB so the per-track ``track_artist``
    # column the resolver just wrote (PR2 Apply #1) flows into the
    # snapshot.
    post_resolve_tracks = db.get_tracks(req_id)
    _generate_plan_after_add(
        db, req_id,
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        tracks=post_resolve_tracks,
        source=source,
        release_group_year=resolved.release_group_year,
        is_va_compilation=resolved.is_va_compilation,
        catalog_number=resolved.catalog_number,
    )


def cmd_status(db, args):
    counts = db.count_by_status()
    if not counts:
        print("  Database is empty.")
        return
    total = sum(counts.values())
    print(f"  Pipeline DB status ({total} total):\n")
    for status in ["wanted", "downloading", "imported", "manual"]:
        c = counts.get(status, 0)
        if c > 0:
            print(f"    {status:15s} {c:4d}")


def cmd_retry(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.to_wanted(from_status=req["status"]),
    )
    print(f"  Reset to wanted: [{args.id}] {req['artist_name']} - {req['album_title']}")


def cmd_cancel(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.to_manual(from_status=req["status"]),
    )
    print(f"  Marked for manual download: [{args.id}] {req['artist_name']} - {req['album_title']}")


VALID_STATUSES = ["wanted", "imported", "manual"]


def cmd_set(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    old_status = req["status"]
    if old_status == args.status:
        print(f"  [{args.id}] already has status '{args.status}'.")
        return
    finalize_request(
        db,
        args.id,
        transitions.RequestTransition.status_only(
            args.status,
            from_status=old_status,
        ),
    )
    print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: {old_status} → {args.status}")


def cmd_set_intent(db, args):
    """Toggle lossless-on-disk intent for a request.

    'lossless' — keep lossless on disk (overrides global verified_lossless_target)
    'default'  — pipeline decides (uses global verified_lossless_target)
    """
    from lib.quality import QUALITY_LOSSLESS, should_clear_lossless_search_override

    target_format = QUALITY_LOSSLESS if args.intent == "lossless" else None

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    if req["status"] == "downloading":
        print(f"  Cannot set intent while album is downloading.")
        return
    old_target = req.get("target_format")
    label = f"{req['artist_name']} - {req['album_title']}"

    if req["status"] == "imported" and target_format:
        # Re-queue to search for lossless source
        min_br = req.get("min_bitrate")
        finalize_request(
            db,
            args.id,
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=min_br,
            ),
        )
        db.update_request_fields(args.id, target_format=target_format)
        print(f"  [{args.id}] {label}: lossless on disk, re-queued for search")
    else:
        update_fields = {"target_format": target_format}
        if should_clear_lossless_search_override(
            new_target_format=target_format,
            old_target_format=old_target,
            search_filetype_override=req.get("search_filetype_override"),
        ):
            update_fields["search_filetype_override"] = None
        db.update_request_fields(args.id, **update_fields)
        action = "lossless on disk" if target_format else "default (pipeline decides)"
        print(f"  [{args.id}] {label}: {action} "
              f"(target_format: {old_target} → {target_format})")


def _fmt_br(kbps):
    """Format a bitrate value for display."""
    if kbps is None:
        return "-"
    return f"{kbps}kbps"


def _fmt_measurement(m, label=""):
    """Format an AudioQualityMeasurement dict for display."""
    if not m:
        return f"{label}(none)"
    parts = [_fmt_br(m.get("min_bitrate_kbps"))]
    if m.get("spectral_grade"):
        sg = m["spectral_grade"]
        if m.get("spectral_bitrate_kbps"):
            sg += f" ~{m['spectral_bitrate_kbps']}kbps"
        parts.append(f"spectral={sg}")
    if m.get("verified_lossless"):
        parts.append("verified_lossless")
    if m.get("was_converted_from"):
        parts.append(f"from {m['was_converted_from']}")
    if m.get("is_cbr"):
        parts.append("CBR")
    return f"{label}{', '.join(parts)}"


def _json_default(value):
    """Serialize common PostgreSQL values for JSON/debug output."""
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _stringify_query_value(value):
    """Format a SQL value for table output."""
    if value is None:
        return "NULL"
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=_json_default, sort_keys=True)
    if isinstance(value, (date, datetime, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _render_query_table(rows, columns):
    """Render SQL query results as a simple aligned table."""
    widths = {col: len(col) for col in columns}
    string_rows = []

    for row in rows:
        rendered = []
        for col in columns:
            text = _stringify_query_value(row.get(col))
            widths[col] = max(widths[col], len(text))
            rendered.append(text)
        string_rows.append(rendered)

    header = " | ".join(col.ljust(widths[col]) for col in columns)
    divider = "-+-".join("-" * widths[col] for col in columns)
    lines = [header, divider]
    for rendered in string_rows:
        lines.append(" | ".join(
            value.ljust(widths[col]) for col, value in zip(columns, rendered)
        ))
    row_label = "row" if len(rows) == 1 else "rows"
    lines.append(f"({len(rows)} {row_label})")
    return lines


def _get_query_sql(args):
    """Resolve SQL text from argv or stdin."""
    sql = sys.stdin.read() if args.sql == "-" else args.sql
    sql = sql.strip()
    if not sql:
        raise ValueError("No SQL provided.")
    return sql


def cmd_query(db, args):
    """Run a debugging SQL query in a read-only session."""
    try:
        sql = _get_query_sql(args)
    except ValueError as exc:
        print(f"  [ERROR] {exc}", file=sys.stderr)
        return 1

    db._execute("SET SESSION default_transaction_read_only = on")
    try:
        cur = db._execute(sql)
        columns = [desc[0] for desc in cur.description] if cur.description else []
        rows = [dict(row) for row in cur.fetchall()] if cur.description else []
    except psycopg2.Error as exc:
        message = exc.pgerror or str(exc)
        print(f"  [ERROR] {message.strip()}", file=sys.stderr)
        return 1
    finally:
        db._execute("SET SESSION default_transaction_read_only = off")

    if args.json:
        print(json.dumps(rows, indent=2, default=_json_default))
        return None

    if not columns:
        print("Query executed successfully.")
        return None

    for line in _render_query_table(rows, columns):
        print(line)
    return None


def _render_import_result(ir_raw):
    """Render an ImportResult JSONB blob as human-readable lines."""
    if not ir_raw:
        return []
    try:
        ir = ir_raw if isinstance(ir_raw, dict) else json.loads(ir_raw)
    except (json.JSONDecodeError, TypeError):
        return []

    lines = []
    decision = ir.get("decision", "?")
    lines.append(f"      decision:  {decision}")

    # v2: measurements
    new_m = ir.get("new_measurement")
    if new_m:
        lines.append(f"      new:       {_fmt_measurement(new_m)}")
        existing_m = ir.get("existing_measurement")
        if existing_m:
            lines.append(f"      existing:  {_fmt_measurement(existing_m)}")

        conv = ir.get("conversion") or {}
        if conv.get("was_converted"):
            src = conv.get("original_filetype", "?")
            tgt = conv.get("target_filetype", "?")
            n = conv.get("converted", 0)
            extra = ""
            if conv.get("is_transcode"):
                extra = " (TRANSCODE)"
            lines.append(f"      converted: {src} -> {tgt} ({n} files){extra}")
    else:
        # v1 fallback
        quality = ir.get("quality") or {}
        spectral = ir.get("spectral") or {}
        if quality.get("new_min_bitrate") is not None:
            lines.append(f"      new:       {_fmt_br(quality['new_min_bitrate'])}")
        if quality.get("prev_min_bitrate") is not None:
            lines.append(f"      existing:  {_fmt_br(quality['prev_min_bitrate'])}")
        if spectral.get("grade"):
            lines.append(f"      spectral:  {spectral['grade']}")

    if ir.get("error"):
        lines.append(f"      error:     {ir['error']}")

    # Issue #130: surface post-import `beet move` failures so operators
    # don't have to grep JSONB to see them. `disambiguated=True` means
    # the move ran cleanly; a `disambiguation_failure` object means it
    # did not exit cleanly and the album is in beets at a stale path.
    pf = ir.get("postflight") or {}
    dfail = pf.get("disambiguation_failure")
    if dfail:
        reason = dfail.get("reason", "unknown")
        detail = dfail.get("detail", "")
        lines.append(f"      disambig:  FAILED ({reason}): {detail}")
    elif pf.get("disambiguated") is True:
        lines.append(f"      disambig:  ok")

    return lines


def _render_search_forensics_summary(
    request_row: dict, latest_search: dict,
) -> list[str]:
    """Build the U7 forensic summary block printed above search history.

    Inputs:
    - ``request_row``: the row dict from ``PipelineDB.get_request``. Used
      for ``manual_reason`` (None when not exhausted / pre-U7).
    - ``latest_search``: the most recent ``search_log`` row dict (already
      ordered newest-first by ``get_search_history``). The candidates
      JSONB blob is decoded here via ``msgspec.convert(blob,
      type=list[CandidateScore])`` — single decode site for the CLI side
      per ``.claude/rules/code-quality.md`` § Wire-boundary types.

    Empty lists / NULL JSONB blobs render gracefully (no
    ``msgspec.ValidationError``); rows without forensic fields (e.g.
    pre-U1 search_log entries) print "no forensic data yet".
    """
    from lib.quality import CandidateScore, top_candidates

    lines: list[str] = ["", "  Search Forensics:"]
    variant = latest_search.get("variant")
    final_state = latest_search.get("final_state")
    manual_reason = request_row.get("manual_reason")

    if variant:
        lines.append(f"    variant:        {variant}")
    if final_state:
        lines.append(f"    final_state:    {final_state}")
    if manual_reason:
        lines.append(f"    manual_reason:  {manual_reason}")

    raw_candidates = latest_search.get("candidates")
    if raw_candidates is None:
        if not (variant or final_state or manual_reason):
            lines.append("    (no forensic data yet)")
        else:
            lines.append("    candidates:     (none captured)")
        return lines

    try:
        candidates = msgspec.convert(raw_candidates, type=list[CandidateScore])
    except msgspec.ValidationError as e:
        # Defensive — production writes via the same Struct so this should
        # never trip in practice, but a corrupted historical row should
        # not crash `pipeline-cli show`.
        lines.append(f"    candidates:     <decode error: {e}>")
        return lines

    if not candidates:
        lines.append("    candidates:     (empty list)")
        return lines

    # Top-3 by (matched_tracks DESC, avg_ratio DESC) for the compact CLI
    # glance; the web long-tail console's "peers seen" panel renders the full
    # stored slice (top-20). Same ranking, different surface depth. Shared
    # ranking lives in lib/quality.py.
    top = top_candidates(candidates, limit=3)
    lines.append(f"    top candidates ({len(top)} of {len(candidates)}):")
    for c in top:
        lines.append(
            f"      {c.username} | {c.dir} | "
            f"{c.matched_tracks}/{c.total_tracks} | "
            f"avg={c.avg_ratio:.2f} | {c.filetype}"
        )
    return lines


def _render_download_history_header(row):
    source = row.get("source") or "slskd"
    outcome = row.get("outcome")
    created_at = row.get("created_at")
    if source == "youtube":
        meta = row.get("youtube_metadata")
        meta = meta if isinstance(meta, dict) else {}
        parts = ["via youtube"]
        browse_id = meta.get("browse_id")
        if browse_id:
            parts.append(f"browse_id={browse_id}")
        observed = meta.get("observed_track_count")
        expected = meta.get("expected_track_count")
        if observed is not None or expected is not None:
            parts.append(f"tracks={observed if observed is not None else '?'}/"
                         f"{expected if expected is not None else '?'}")
        reason = meta.get("reason")
        if reason:
            parts.append(f"reason={reason}")
        return f"    [{created_at}] {outcome} {' '.join(parts)}"
    username = row.get("soulseek_username") or "-"
    return (
        f"    [{created_at}] {outcome} via slskd from {username} "
        f"(dist={row.get('beets_distance')})"
    )


def _render_youtube_metadata(row):
    if (row.get("source") or "slskd") != "youtube":
        return []
    meta = row.get("youtube_metadata")
    if not isinstance(meta, dict):
        return []
    lines = []
    yt_url = meta.get("yt_url")
    if yt_url:
        lines.append(f"      yt_url:    {yt_url}")
    stderr = meta.get("stderr_excerpt")
    if stderr:
        lines.append(f"      stderr:    {str(stderr).splitlines()[-1]}")
    cleanup_error = meta.get("cleanup_error")
    if cleanup_error:
        lines.append(f"      cleanup_error: {cleanup_error}")
    return lines


def cmd_show(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return

    print(f"  ID:           {req['id']}")
    print(f"  Artist:       {req['artist_name']}")
    print(f"  Album:        {req['album_title']}")
    print(f"  Status:       {req['status']}")
    print(f"  Source:       {req['source']}")
    print(f"  MB Release:   {req['mb_release_id']}")
    print(f"  MB RG:        {req['mb_release_group_id']}")
    print(f"  MB Artist:    {req['mb_artist_id']}")
    print(f"  Discogs:      {req['discogs_release_id']}")
    print(f"  Year:         {req['year']}")
    print(f"  RG Year:      {req.get('release_group_year') or '-'}")
    print(f"  Country:      {req['country']}")
    print(f"  Format:       {req['format']}")
    print(f"  VA Comp:      {'yes' if req.get('is_va_compilation') else 'no'}")
    print(f"  Catalog #:    {req.get('catalog_number') or '-'}")
    print(f"  Source Path:  {req['source_path']}")
    if req.get("reasoning"):
        print(f"  Reasoning:    {req['reasoning'][:120]}...")
    print(f"  Distance:     {req['beets_distance']}")
    print(f"  Imported:     {req['imported_path']}")
    print(f"  Attempts:     search={req['search_attempts']} dl={req['download_attempts']} val={req['validation_attempts']}")
    print(f"  Created:      {req['created_at']}")
    print(f"  Updated:      {req['updated_at']}")

    # --- Active download state ---
    ads = req.get("active_download_state")
    if ads and isinstance(ads, dict):
        enq = ads.get("enqueued_at", "?")
        ftype = ads.get("filetype", "?")
        fcount = len(ads.get("files", []))
        print(f"\n  Active Download:")
        print(f"    filetype:     {ftype}")
        print(f"    enqueued_at:  {enq}")
        print(f"    files:        {fcount}")

    # --- Quality state ---
    min_br = req.get("min_bitrate")
    prev_br = req.get("prev_min_bitrate")
    verified = req.get("verified_lossless")
    s_grade = req.get("last_download_spectral_grade")
    s_br = req.get("last_download_spectral_bitrate")
    cur_grade = req.get("current_spectral_grade")
    cur_br = req.get("current_spectral_bitrate")
    q_override = req.get("search_filetype_override")
    has_quality = any(
        v is not None
        for v in [min_br, prev_br, verified, s_grade, s_br, cur_grade, cur_br, q_override]
    )
    if has_quality:
        print(f"\n  Quality:")
        print(f"    min_bitrate:        {_fmt_br(min_br)}")
        if prev_br is not None:
            print(f"    prev_min_bitrate:   {_fmt_br(prev_br)}")
        print(f"    verified_lossless:  {verified or False}")
        if s_grade:
            sg = s_grade
            if s_br:
                sg += f" ~{s_br}kbps"
            print(f"    last_download:     {sg}")
        if cur_grade:
            current = cur_grade
            if cur_br:
                current += f" ~{cur_br}kbps"
            print(f"    current_spectral:  {current}")
        if q_override:
            print(f"    search_filetype_override: {q_override}")

    tracks = db.get_tracks(req['id'])
    if tracks:
        print(f"\n  Tracks ({len(tracks)}):")
        for t in tracks:
            dur = f"{t['length_seconds']:.0f}s" if t['length_seconds'] else "?"
            print(f"    {t['disc_number']}.{t['track_number']:02d} {t['title']} ({dur})")

    searches = db.get_search_history(req['id'])
    if searches:
        # U7: print a forensic summary above the row table — the most
        # recent variant + final_state, the request's manual_reason if
        # populated, and the top-3 candidates from the latest search_log
        # row's JSONB blob. The blob is decoded once via msgspec.convert
        # per single-decode-site discipline (code-quality.md § Wire-
        # boundary types). Older rows / NULL blobs render gracefully.
        for line in _render_search_forensics_summary(req, searches[0]):
            print(line)
        print(f"\n  Search History ({len(searches)}):")
        for s in searches:
            q = s['query'] or "(no query)"
            rc = s['result_count']
            rc_str = f"{rc} results" if rc is not None else "n/a"
            el = s['elapsed_s']
            el_str = f"{el:.1f}s" if el is not None else ""
            variant = s.get('variant') or "-"
            print(f"    [{s['created_at']}] {s['outcome']:12s} {variant:14s} {rc_str:>12s} {el_str:>6s}  {q}")

    history = db.get_download_history(req['id'])
    if history:
        print(f"\n  Download History ({len(history)}):")
        for h in history:
            print(_render_download_history_header(h))
            for line in _render_youtube_metadata(h):
                print(line)
            for line in _render_import_result(h.get("import_result")):
                print(line)

    denied = db.get_denylisted_users(req['id'])
    if denied:
        print(f"\n  Denylisted Users ({len(denied)}):")
        for d in denied:
            print(f"    {d['username']}: {d['reason']}")



def cmd_quality(db, args):
    """Show quality state and simulate decisions for common download scenarios."""
    from lib.quality import (full_pipeline_decision, quality_gate_decision,
                             AudioQualityMeasurement, gate_rank,
                             rejection_backfill_override,
                             search_tiers, compute_effective_override_bitrate)

    rank_cfg = _load_runtime_rank_config()

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return

    label = f"{req['artist_name']} - {req['album_title']}"
    min_br = req.get("min_bitrate")
    verified = bool(req.get("verified_lossless"))
    current_br = req.get("current_spectral_bitrate")
    q_override = req.get("search_filetype_override")
    spectral_grade = req.get("current_spectral_grade")
    final_format = req.get("final_format")
    target_format = req.get("target_format")
    verified_lossless_target = _load_runtime_verified_lossless_target() or None
    # Existing-side lossless-source V0 probe — anchors the lossless_source_locked
    # rule. When set, lossy candidates short-circuit to reject inside the
    # provisional lane regardless of how their on-disk avg compares.
    existing_v0_probe_avg = req.get("current_lossless_source_v0_probe_avg_bitrate")

    print(f"  {label}")
    print(f"  Status: {req['status']}")
    print(f"  Rank config: metric={rank_cfg.bitrate_metric.value}, "
          f"gate_min_rank={rank_cfg.gate_min_rank.name}")
    print(f"  Verified-lossless output: "
          f"{_quality_preview_target_label(target_format, verified_lossless_target)}")
    print()

    # --- Current quality gate ---
    is_cbr = False
    avg_br = None
    median_br = None
    existing_format_hint = final_format
    if min_br is not None:
        mbid = req.get("mb_release_id")
        info = _load_beets_album_info(mbid, rank_cfg)
        if info:
            is_cbr = info.is_cbr
            avg_br = info.avg_bitrate_kbps
            median_br = info.median_bitrate_kbps
            if not existing_format_hint:
                existing_format_hint = info.format
        gate_spectral_br = None
        effective_gate_br = compute_effective_override_bitrate(
            min_br, current_br, spectral_grade)
        if (min_br is not None and effective_gate_br is not None
                and effective_gate_br < min_br):
            gate_spectral_br = current_br
        current = AudioQualityMeasurement(
            min_bitrate_kbps=min_br,
            avg_bitrate_kbps=avg_br,
            median_bitrate_kbps=median_br,
            format=existing_format_hint or "MP3",
            is_cbr=is_cbr,
            verified_lossless=verified,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=gate_spectral_br)
        # gate_rank centralizes the spectral clamp the gate applies, so the
        # displayed label always matches the verdict (no more EXCELLENT next
        # to NEEDS UPGRADE on a fake CBR 320).
        current_rank = gate_rank(current, rank_cfg)
        gate = quality_gate_decision(current, cfg=rank_cfg)
        gate_label = {"accept": "DONE", "requeue_upgrade": "NEEDS UPGRADE",
                      "requeue_lossless": "NEEDS LOSSLESS"}[gate]
        print(f"  Quality gate:  {gate_label}  (rank={current_rank.name})")
        print(f"    min_bitrate={_fmt_br(min_br)}, "
              f"avg_bitrate={_fmt_br(avg_br) if avg_br else 'n/a'}, "
              f"median_bitrate={_fmt_br(median_br) if median_br else 'n/a'}, "
              f"format={existing_format_hint or '(unknown)'}, "
              f"verified_lossless={verified}, is_cbr={is_cbr}")
        if current_br:
            print(f"    current_spectral_bitrate={current_br}kbps")
        if spectral_grade:
            print(f"    current_spectral_grade={spectral_grade}")
        if existing_v0_probe_avg is not None:
            print(f"    current_lossless_source_v0_probe_avg={existing_v0_probe_avg}kbps "
                  f"(locks lossy candidates)")
        if q_override:
            print(f"    searching: {q_override}")
    else:
        print(f"  Quality gate:  NO DATA (not yet imported)")

    # --- Rejection backfill status ---
    backfill = rejection_backfill_override(
        is_cbr=is_cbr, min_bitrate_kbps=min_br,
        spectral_grade=spectral_grade, verified_lossless=verified,
        cfg=rank_cfg)
    if backfill and not q_override:
        print(f"  Backfill:      would set search_filetype_override='{backfill}' on next rejection")
    elif q_override:
        print(f"  Backfill:      not needed (search_filetype_override already set)")
    else:
        print(f"  Backfill:      won't fire (conditions not met)")

    # --- Simulate common scenarios ---
    effective_existing = compute_effective_override_bitrate(
        min_br, current_br, spectral_grade)
    override_min_bitrate = None
    if (effective_existing is not None and min_br is not None
            and effective_existing != min_br):
        override_min_bitrate = effective_existing

    lossless_target_label = _quality_preview_target_label(
        target_format, verified_lossless_target)
    scenarios = [
        # --- FLAC downloads ---
        (f"Genuine FLAC → {lossless_target_label} (high bitrate)", dict(
            is_flac=True, min_bitrate=245, is_cbr=False,
            spectral_grade="genuine", converted_count=12,
            post_conversion_min_bitrate=245)),
        (f"Genuine FLAC → {lossless_target_label} (lo-fi, 207kbps)", dict(
            is_flac=True, min_bitrate=207, is_cbr=False,
            spectral_grade="genuine", converted_count=12,
            post_conversion_min_bitrate=207)),
        (f"Marginal FLAC → {lossless_target_label}", dict(
            is_flac=True, min_bitrate=240, is_cbr=False,
            spectral_grade="marginal", converted_count=12,
            post_conversion_min_bitrate=240)),
        ("Suspect FLAC (transcode, 190kbps)", dict(
            is_flac=True, min_bitrate=190, is_cbr=False,
            spectral_grade="suspect", converted_count=12,
            post_conversion_min_bitrate=190)),
        ("Suspect FLAC (transcode, 245kbps)", dict(
            is_flac=True, min_bitrate=245, is_cbr=False,
            spectral_grade="suspect", converted_count=12,
            post_conversion_min_bitrate=245)),
        # Bill Hicks 1990 "Dangerous" shape: spoken-word lossless that
        # spectral_check false-positives as suspect (high HF deficit
        # against music-tuned thresholds), but the lossless_source_v0
        # probe corroborates a genuine master. The V0-avg trust override
        # in determine_verified_lossless flips this to verified.
        ("Suspect FLAC + lossless_source_v0 avg=241/min=219 (V0 override)", dict(
            is_flac=True, min_bitrate=219, is_cbr=False,
            spectral_grade="suspect", converted_count=10,
            post_conversion_min_bitrate=219,
            candidate_v0_probe_avg=241,
            candidate_v0_probe_min=219,
            candidate_v0_probe_kind="lossless_source_v0")),
        # --- MP3 VBR downloads ---
        # avg_bitrate drives the new preimport spectral gate (issue #93):
        # VBR with avg >= cfg.mp3_vbr.excellent skips spectral entirely,
        # below gates through analysis even without a spectral_grade input.
        ("MP3 V0 genuine (avg 245kbps, gate skips)", dict(
            is_flac=False, min_bitrate=240, is_cbr=False,
            is_vbr=True, avg_bitrate=245)),
        ("MP3 V0 (low, avg 205kbps, gate runs)", dict(
            is_flac=False, min_bitrate=205, is_cbr=False,
            is_vbr=True, avg_bitrate=205)),
        ("VBR transcode (Go! Team shape, avg 182kbps)", dict(
            is_flac=False, min_bitrate=126, is_cbr=False,
            is_vbr=True, avg_bitrate=182,
            spectral_grade="likely_transcode", spectral_bitrate=96)),
        ("MP3 V2 (avg 190kbps, gate runs)", dict(
            is_flac=False, min_bitrate=190, is_cbr=False,
            is_vbr=True, avg_bitrate=190)),
        # --- MP3 CBR downloads (no spectral) ---
        ("CBR 320 (no spectral)", dict(
            is_flac=False, min_bitrate=320, is_cbr=True)),
        ("CBR 256 (no spectral)", dict(
            is_flac=False, min_bitrate=256, is_cbr=True)),
        ("CBR 192 (no spectral)", dict(
            is_flac=False, min_bitrate=192, is_cbr=True)),
        # --- MP3 CBR downloads (with spectral) ---
        ("CBR 320 genuine", dict(
            is_flac=False, min_bitrate=320, is_cbr=True,
            spectral_grade="genuine")),
        ("CBR 320 suspect (~128kbps)", dict(
            is_flac=False, min_bitrate=320, is_cbr=True,
            spectral_grade="suspect", spectral_bitrate=128)),
        ("CBR 320 suspect (~192kbps)", dict(
            is_flac=False, min_bitrate=320, is_cbr=True,
            spectral_grade="suspect", spectral_bitrate=192)),
        ("CBR 256 genuine", dict(
            is_flac=False, min_bitrate=256, is_cbr=True,
            spectral_grade="genuine")),
        ("CBR 192 genuine", dict(
            is_flac=False, min_bitrate=192, is_cbr=True,
            spectral_grade="genuine")),
    ]
    # --- Preimport gate scenarios (issue #91) ---
    # Audio and nested-layout gates short-circuit before any FLAC/MP3 stage
    # runs. These let operators see the rejection paths that live in
    # lib.measurement.measure_preimport_state and
    # lib.import_dispatch.dispatch_import_from_db.
    #
    # `audio_check_mode` is read from the active runtime config and
    # applied to every scenario — on deployments with
    # `[Beets Validation] audio_check = off`, ALL scenarios must report
    # `preimport_audio=skipped_off`, not just the synthetic preimport
    # ones (Codex round 3 P2). Scenarios that explicitly want to
    # demonstrate the gate (e.g. the audio_corrupt demo) override this
    # value.
    runtime_audio_check = _load_runtime_audio_check_mode()
    scenarios.extend([
        # `audio_check_mode` not set here — defaults to the runtime value
        # below so the scenario honestly reflects the deployment: on an
        # `audio_check = off` deployment this prints `skipped_off`, which
        # is what the live pipeline would do (Codex round 2 P3 + round 3 P2).
        ("PREIMPORT: Audio corrupt (ffmpeg fail)", dict(
            is_flac=False, min_bitrate=256, is_cbr=False,
            audio_corrupt=True)),
        ("PREIMPORT: Force-import with nested folders", dict(
            is_flac=False, min_bitrate=320, is_cbr=True,
            import_mode="force", has_nested_audio=True)),
    ])

    print(f"\n  What would happen if we downloaded:")
    for name, params in scenarios:
        # Apply runtime audio_check_mode as a default; scenarios that
        # explicitly override it still win (dict unpack order).
        params_with_runtime = {
            "audio_check_mode": runtime_audio_check,
            **params,
        }
        result = full_pipeline_decision(
            existing_min_bitrate=min_br,
            # Forward avg_bitrate too — under the default AVG policy the
            # simulator must compare against the real album avg, not min,
            # or VBR albums rank at the wrong tier in stage 2/3 output
            # (issue #93 codex round 4).
            existing_avg_bitrate=avg_br,
            existing_spectral_grade=spectral_grade,
            existing_spectral_bitrate=current_br,
            override_min_bitrate=override_min_bitrate,
            existing_format=existing_format_hint,
            existing_is_cbr=is_cbr,
            verified_lossless=verified,
            target_format=target_format,
            verified_lossless_target=verified_lossless_target,
            existing_v0_probe_avg=existing_v0_probe_avg,
            cfg=rank_cfg,
            **params_with_runtime)

        imported = "IMPORT" if result["imported"] else "REJECT"
        parts = [imported]
        if result["denylisted"]:
            parts.append("denylist")
        if result["keep_searching"]:
            parts.append("keep searching")
        final = result["final_status"] or "?"
        decision_chain = " → ".join(
            f"{s}={result[s]}"
            for s in ["preimport_audio", "preimport_nested",
                      "stage0_spectral_gate", "stage1_spectral",
                      "stage2_import", "stage3_quality_gate"]
            if result[s] is not None)

        print(f"    {name}:")
        print(f"      → {', '.join(parts)} (final: {final})")
        if decision_chain:
            print(f"      chain: {decision_chain}")

        # For rejections that keep searching: simulate what happens after
        if not result["imported"] and result["keep_searching"]:
            if q_override:
                tiers, _ = search_tiers(q_override, [])
                print(f"      next search: {', '.join(tiers)}")
            else:
                # Simulate spectral propagation: on downgrade rejection,
                # the download's spectral would be written to on-disk state.
                # Use the download's spectral_grade to compute the backfill.
                dl_spectral = params.get("spectral_grade")
                propagated = rejection_backfill_override(
                    is_cbr=is_cbr,
                    min_bitrate_kbps=min_br,
                    spectral_grade=dl_spectral if dl_spectral else spectral_grade,
                    verified_lossless=verified,
                    cfg=rank_cfg,
                )
                if propagated:
                    tiers, _ = search_tiers(propagated, [])
                    print(f"      backfill → override='{propagated}'"
                          f" (next: {', '.join(tiers)})")
                else:
                    print(f"      no backfill"
                          f" (spectral={dl_spectral or spectral_grade or 'none'},"
                          f" keep all tiers)")


IMPORT_ONE = os.path.join(os.path.dirname(__file__), "..", "harness", "import_one.py")

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

    vr = vr_raw if isinstance(vr_raw, dict) else json.loads(vr_raw)
    failed_path = vr.get("failed_path")
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


def cmd_manual_import(db, args):
    """Import a local folder as a pipeline request."""
    request_id = args.id
    path = args.path

    # 1. Look up request
    req = db.get_request(request_id)
    if not req:
        print(f"  Request {request_id} not found.")
        return

    mbid = req["mb_release_id"]
    if not mbid:
        print(f"  Request {request_id} has no MusicBrainz release ID.")
        return

    # 2. Resolve and verify the path — matches cmd_force_import so a
    # manual-import can accept the same relative paths (e.g.
    # "failed_imports/Foo") without requiring the user to pre-absolutize.
    resolved_path = _resolve_failed_path(path)
    if not resolved_path:
        print(f"  Files not found at: {path}")
        if not os.path.isabs(path):
            print(f"  (also tried: {', '.join(os.path.join(b, path) for b in SLSKD_DOWNLOAD_DIRS)})")
        return
    path = resolved_path

    print(f"  Manual import: {req['artist_name']} - {req['album_title']}")
    print(f"  Path: {path}")
    print(f"  MBID: {mbid}")

    job = db.enqueue_import_job(
        IMPORT_JOB_MANUAL,
        request_id=request_id,
        dedupe_key=manual_import_dedupe_key(request_id, path),
        payload=manual_import_payload(failed_path=path),
        message=f"Manual import queued for {req['artist_name']} - {req['album_title']}",
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


def cmd_repair_spectral(db, args):
    """Find and repair albums stuck by stale current_spectral_bitrate.

    Identifies wanted albums where current_spectral_grade is genuine but
    current_spectral_bitrate still holds a stale transcode estimate,
    causing the quality gate to requeue indefinitely (issue #18).
    """
    from lib.import_dispatch import load_quality_gate_state
    from lib.quality import quality_gate_decision

    rank_cfg = _load_runtime_rank_config()

    # Find candidates: genuine on disk but spectral bitrate < min_bitrate
    # (genuine files should have no spectral cliff → bitrate should be NULL)
    cur = db._execute("""
        SELECT id, artist_name, album_title, min_bitrate,
               current_spectral_bitrate, current_spectral_grade,
               last_download_spectral_bitrate, last_download_spectral_grade,
               verified_lossless
        FROM album_requests
        WHERE status = 'wanted'
          AND current_spectral_grade = 'genuine'
          AND current_spectral_bitrate IS NOT NULL
    """)
    candidates = [dict(r) for r in cur.fetchall()]

    if not candidates:
        print("No stuck albums found.")
        return

    print(f"Found {len(candidates)} album(s) with stale spectral data:\n")

    repaired = 0
    for req in candidates:
        rid = req["id"]
        label = f"{req['artist_name']} - {req['album_title']}"
        stale_br = req["current_spectral_bitrate"]
        state = load_quality_gate_state(
            request_id=rid,
            db=db,
            quality_ranks=rank_cfg,
        )
        effective_min_br = (
            state.min_bitrate_kbps
            if state is not None
            else req["min_bitrate"]
        )
        print(f"  [{rid:>4}] {label}")
        print(f"         min_bitrate={effective_min_br}kbps, "
              f"stale current_spectral={stale_br}kbps")

        # Check what quality gate would decide after clearing stale data
        decision = (
            quality_gate_decision(state.measurement, cfg=rank_cfg)
            if state is not None
            else "requeue_upgrade"
        )
        print(f"         after repair: quality_gate_decision → {decision}")

        if args.dry_run:
            print(f"         [DRY RUN] would clear spectral + remove stale denylists")
            continue

        # Clear stale spectral fields
        db._execute("""
            UPDATE album_requests
            SET last_download_spectral_bitrate = NULL,
                current_spectral_bitrate = NULL,
                updated_at = NOW()
            WHERE id = %s
        """, (rid,))

        # Remove denylist entries caused by stale spectral
        del_cur = db._execute("""
            DELETE FROM source_denylist
            WHERE request_id = %s
              AND (reason LIKE 'quality gate: spectral%%'
                   OR reason LIKE 'spectral:%%')
            RETURNING username, reason
        """, (rid,))
        removed = del_cur.fetchall()
        for entry in removed:
            print(f"         un-denylisted: {entry['username']} ({entry['reason']})")

        # If quality gate would accept, transition to imported
        if decision == "accept" and effective_min_br is not None:
            finalize_request(
                db,
                rid,
                transitions.RequestTransition.to_imported(
                    from_status="wanted",
                    min_bitrate=effective_min_br,
                ),
            )
            print(f"         → transitioned to imported")
        else:
            print(f"         → remains wanted (gate says {decision})")

        repaired += 1

    print(f"\nRepaired {repaired} album(s)." if not args.dry_run
          else f"\n[DRY RUN] Would repair {len(candidates)} album(s).")


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
        "import_mode",
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
                source_username=args.source_username,
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


def cmd_wrong_match_triage(db, args):
    """Run evidence-only cleanup for the full Wrong Matches queue."""
    from lib.wrong_match_cleanup_service import (
        OUTCOME_KEYS,
        cleanup_all_wrong_matches,
    )

    forbidden_scope: list[str] = []
    for name in ("download_log_id", "request_id", "limit", "all"):
        value = getattr(args, name, None)
        if value is not None and value is not False:
            forbidden_scope.append(f"--{name.replace('_', '-')}")
    if forbidden_scope:
        print(
            "  wrong-match-triage processes the whole Wrong Matches queue; "
            f"scope flags are not supported: {', '.join(forbidden_scope)}.",
            file=sys.stderr,
        )
        return 2
    if not args.apply:
        print(
            "  Refusing destructive wrong-match triage without --apply. "
            "This command processes the whole Wrong Matches queue.",
            file=sys.stderr,
        )
        return 2

    summary = cleanup_all_wrong_matches(db, confirm_all_wrong_matches=True)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return 0

    for result in summary.results:
        print(
            f"  [{result.download_log_id}] {result.outcome}"
            f"{': ' + result.reason if result.reason else ''}"
        )
    if summary.results:
        print("")
    for outcome in OUTCOME_KEYS:
        print(f"  {outcome}: {getattr(summary, outcome)}")
    print(f"  total: {summary.processed}")
    return 0


def _print_wrong_match_delete_result(result, *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return
    print(f"  [{result.download_log_id}] {result.outcome}")
    if result.reason:
        print(f"  reason: {result.reason}")
    if result.deleted_path:
        print(f"  deleted_path: {result.deleted_path}")
    if result.path_missing:
        print("  path_missing: yes")
    print(f"  cleared_rows: {result.cleared_rows}")


def cmd_wrong_match_delete(db, args):
    """Delete one visible Wrong Matches source folder."""
    from lib.wrong_match_delete_service import (
        OUTCOME_DELETE_FAILED,
        OUTCOME_DELETED,
        OUTCOME_SKIPPED_ACTIVE_JOB,
        OUTCOME_SKIPPED_INVALID_ROW,
        OUTCOME_SKIPPED_LOCKED,
        OUTCOME_SKIPPED_NOT_VISIBLE,
        OUTCOME_SKIPPED_UNSAFE_PATH,
        delete_wrong_match,
    )

    if not args.apply:
        print(
            "  Refusing destructive wrong-match delete without --apply.",
            file=sys.stderr,
        )
        return 2

    result = delete_wrong_match(
        db,
        args.download_log_id,
        require_visible=True,
    )
    _print_wrong_match_delete_result(result, json_output=args.json)
    if result.outcome == OUTCOME_DELETED:
        return 0
    if result.outcome in (OUTCOME_SKIPPED_INVALID_ROW, OUTCOME_SKIPPED_NOT_VISIBLE):
        return 2
    if result.outcome == OUTCOME_SKIPPED_ACTIVE_JOB:
        return 4
    if result.outcome == OUTCOME_SKIPPED_UNSAFE_PATH:
        return 3
    if result.outcome == OUTCOME_SKIPPED_LOCKED:
        return 5
    if result.outcome == OUTCOME_DELETE_FAILED:
        return 1
    return 1


def cmd_wrong_match_delete_group(db, args):
    """Delete every visible Wrong Matches source folder for one request."""
    from lib.wrong_match_delete_service import delete_wrong_match_group

    if not args.apply:
        print(
            "  Refusing destructive wrong-match group delete without --apply.",
            file=sys.stderr,
        )
        return 2

    summary = delete_wrong_match_group(db, args.request_id)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return _wrong_match_delete_group_exit_code(summary)

    for result in summary.results:
        print(f"  [{result.download_log_id}] {result.outcome}")
        if result.reason:
            print(f"    reason: {result.reason}")
    if summary.results:
        print("")
    print(f"  deleted: {summary.deleted}")
    print(f"  deleted_paths: {summary.deleted_paths}")
    print(f"  cleared: {summary.cleared}")
    print(f"  skipped: {summary.skipped}")
    print(f"  errors: {summary.errors}")
    print(f"  remaining: {summary.remaining}")
    return _wrong_match_delete_group_exit_code(summary)


def _wrong_match_delete_group_exit_code(summary) -> int:
    from lib.wrong_match_delete_service import (
        OUTCOME_DELETE_FAILED,
        OUTCOME_SKIPPED_ACTIVE_JOB,
        OUTCOME_SKIPPED_INVALID_ROW,
        OUTCOME_SKIPPED_LOCKED,
        OUTCOME_SKIPPED_NOT_VISIBLE,
        OUTCOME_SKIPPED_UNSAFE_PATH,
    )

    if summary.success:
        return 0
    outcomes = {result.outcome for result in summary.results}
    if OUTCOME_DELETE_FAILED in outcomes:
        return 1
    if OUTCOME_SKIPPED_LOCKED in outcomes:
        return 5
    if OUTCOME_SKIPPED_ACTIVE_JOB in outcomes:
        return 4
    if OUTCOME_SKIPPED_UNSAFE_PATH in outcomes:
        return 3
    if outcomes & {OUTCOME_SKIPPED_INVALID_ROW, OUTCOME_SKIPPED_NOT_VISIBLE}:
        return 2
    return 1


def cmd_search_plan_show(db, args):
    """U6: read-only `pipeline-cli search-plan show <id>`.

    Default: human-readable text including the U8 stats section. Pass
    ``--no-stats`` to suppress stats (useful for legacy assertions /
    scripts that want only the static plan dump). ``--json``: same
    payload the web route emits, useful for scripting / future
    dashboard parity. Exit code 2 on missing request, 0 on found.
    """
    from lib.search_plan_inspection import (
        RequestNotFound,
        build_inspection_payload,
        render_human_lines,
    )

    include_stats = not getattr(args, "no_stats", False)
    payload = build_inspection_payload(
        db, int(args.id), include_stats=include_stats)
    if isinstance(payload, RequestNotFound):
        if getattr(args, "json", False):
            print(json.dumps({
                "error": "Not found",
                "request_id": payload.request_id,
            }, indent=2, sort_keys=True))
            return 2
        print(f"  Request {payload.request_id} not found.")
        return 2
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0
    for line in render_human_lines(payload):
        print(line)
    return 0


def cmd_search_plan_regenerate(db, args):
    """U8: ``pipeline-cli search-plan regenerate <request_id>``.

    Wraps ``SearchPlanService.generate_for_request(regenerate=True)``
    so the CLI never hand-rolls plan persistence. Allowed for any
    request status, but only ``wanted`` requests with a successful
    active plan are executable — the output makes that explicit.

    Exit codes:
      * 0 — ``RESULT_SUCCESS`` or ``RESULT_NOOP_ACTIVE_PLAN_EXISTS``
        (the latter only when called without ``--regenerate``-style
        force; the service treats explicit regeneration as always
        attempting).
      * 2 — ``RESULT_REQUEST_NOT_FOUND`` (matches search-plan show).
      * 3 — ``RESULT_FAILED_DETERMINISTIC`` (sticky failure; old
        active plan preserved).
      * 4 — ``RESULT_FAILED_TRANSIENT`` (retryable; old active plan
        preserved).
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        RESULT_FAILED_DETERMINISTIC,
        RESULT_FAILED_TRANSIENT,
        RESULT_NOOP_ACTIVE_PLAN_EXISTS,
        RESULT_REQUEST_NOT_FOUND,
        RESULT_SUCCESS,
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.generate_for_request(
        int(args.id),
        regenerate=True,
        prepend_artist=getattr(args, "prepend_artist", None),
    )

    payload = {
        "request_id": int(args.id),
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "is_supersede": result.is_supersede,
        "failure_class": result.failure_class,
        "error_message": result.error_message,
    }
    # Add an executability hint so operators don't misread "200 / success"
    # on an imported/manual request as "now downloading".
    req = db.get_request(int(args.id))
    if req is not None:
        payload["request_status"] = req.get("status")
        payload["executable"] = (
            req.get("status") == "wanted"
            and result.outcome == RESULT_SUCCESS
        )
    else:
        payload["request_status"] = None
        payload["executable"] = False

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.plan_id is not None:
            print(f"  New plan id:       {result.plan_id}")
        if result.is_supersede:
            print("  Replaced previous active plan: yes")
        if result.failure_class:
            print(f"  Failure class:     {result.failure_class}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")
        print(f"  Request status:    {payload['request_status'] or '-'}")
        print(f"  Executable:        {'yes' if payload['executable'] else 'no'}")
        if not payload["executable"] and result.outcome == RESULT_SUCCESS:
            print("  Note: only `wanted` requests run searches; the new "
                  "plan is recorded but will not be executed for this status.")

    if result.outcome == RESULT_SUCCESS:
        return 0
    if result.outcome == RESULT_NOOP_ACTIVE_PLAN_EXISTS:
        return 0
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        return 2
    if result.outcome == RESULT_FAILED_DETERMINISTIC:
        return 3
    if result.outcome == RESULT_FAILED_TRANSIENT:
        return 4
    # Defensive fallback for any future outcome string.
    return 1


def cmd_replace(db, args):
    """Supersede a request with a new row at a different MBID.

    Counterpart of ``POST /api/pipeline/<id>/replace``. Both surfaces
    wrap ``MbidReplaceService.replace_request_mbid`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_REPLACED``
      * 2 — ``RESULT_NOT_FOUND``
      * 3 — ``RESULT_TARGET_INVALID``, ``RESULT_TARGET_RELEASE_GROUP_MISMATCH``,
            ``RESULT_TARGET_SAME_AS_CURRENT`` (semantic input violations)
      * 4 — ``RESULT_WRONG_STATE`` (including supersede race —
            double-click landed first; descendant_request_id is set),
            ``RESULT_TARGET_COLLISION_REQUEST``
      * 5 — ``RESULT_TRANSIENT`` (retryable; MB-mirror unreachable etc.)
    """
    from lib.config import read_runtime_config
    from lib.mbid_replace_service import (
        MbidReplaceService,
        RESULT_NOT_FOUND,
        RESULT_REPLACED,
        RESULT_TARGET_COLLISION_REQUEST,
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
        RESULT_TRANSIENT,
        RESULT_WRONG_STATE,
    )

    cfg = read_runtime_config()
    svc = MbidReplaceService(db=db, config=cfg)
    result = svc.replace_request_mbid(
        int(args.id),
        target_mb_release_id=args.target_mb_release_id,
    )

    payload = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "new_request_id": result.new_request_id,
        "current_status": result.current_status,
        "descendant_request_id": result.descendant_request_id,
        "error_message": result.error_message,
        "warnings": list(result.warnings),
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.new_request_id is not None:
            print(f"  New request id:    {result.new_request_id}")
        if result.current_status is not None:
            print(f"  Holder status:     {result.current_status}")
        if result.descendant_request_id is not None:
            print(f"  Descendant id:     {result.descendant_request_id}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")
        if result.warnings:
            print("  Warnings:")
            for w in result.warnings:
                print(f"    - {w}")

    if result.outcome == RESULT_REPLACED:
        return 0
    if result.outcome == RESULT_NOT_FOUND:
        return 2
    if result.outcome in (
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
    ):
        return 3
    if result.outcome in (
        RESULT_WRONG_STATE,
        RESULT_TARGET_COLLISION_REQUEST,
    ):
        return 4
    if result.outcome == RESULT_TRANSIENT:
        return 5
    return 1


def cmd_beets_distance(db, args):
    """Real beets-distance between a download_log's failed_path and an MBID.

    Counterpart of ``GET /api/beets-distance/<download_log_id>/<mbid>``.
    Both surfaces wrap ``lib.beets_distance.compute_beets_distance`` —
    keep them in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface
    symmetry").

    Exit codes:
      * 0 — ``ok``
      * 2 — ``download_log_not_found``, ``request_not_found``
      * 3 — ``mb_no_release_group``, ``wrong_release_group`` (semantic
            input violations, including the cross-RG guardrail)
      * 4 — ``folder_missing``, ``no_audio`` (the artifacts we wanted
            to compare are gone)
      * 5 — ``mb_lookup_failed`` (transient MB-mirror failure)
      * 1 — ``distance_failed`` / unknown outcome
    """
    from lib.beets_distance import compute_beets_distance
    from web import mb as mb_api

    result = compute_beets_distance(
        int(args.download_log_id),
        args.mbid,
        pdb=db,
        mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
        cache=None,
    )

    payload = {
        "outcome": result.outcome,
        "distance": result.distance,
        "matched_tracks": result.matched_tracks,
        "total_local_tracks": result.total_local_tracks,
        "total_mb_tracks": result.total_mb_tracks,
        "extra_local_tracks": result.extra_local_tracks,
        "extra_mb_tracks": result.extra_mb_tracks,
        "components": result.components,
        "request_release_group_id": result.request_release_group_id,
        "candidate_release_group_id": result.candidate_release_group_id,
        "candidate_mbid": result.candidate_mbid,
        "download_log_id": result.download_log_id,
        "request_id": result.request_id,
        "folder_path": result.folder_path,
        "error_message": result.error_message,
        "duration_ms": result.duration_ms,
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  download_log_id:        {result.download_log_id}")
        print(f"  request_id:             {result.request_id}")
        print(f"  candidate_mbid:         {result.candidate_mbid}")
        print(f"  outcome:                {result.outcome}")
        if result.distance is not None:
            print(f"  distance:               {result.distance:.4f}")
        if result.matched_tracks is not None:
            print(f"  matched tracks:         "
                  f"{result.matched_tracks} / {result.total_mb_tracks} "
                  f"({result.total_local_tracks} local)")
        if result.components:
            print("  components:")
            for k, v in sorted(result.components.items()):
                print(f"    {k:<24} {v:.4f}")
        if result.folder_path:
            print(f"  folder:                 {result.folder_path}")
        if result.duration_ms is not None:
            print(f"  latency:                {result.duration_ms} ms")
        if result.error_message:
            print(f"  error:                  {result.error_message}")

    if result.outcome == "ok":
        return 0
    if result.outcome in ("download_log_not_found", "request_not_found"):
        return 2
    if result.outcome in ("mb_no_release_group", "wrong_release_group"):
        return 3
    if result.outcome in ("folder_missing", "no_audio"):
        return 4
    if result.outcome == "mb_lookup_failed":
        return 5
    return 1


class _RedisYoutubeCache:
    """Adapt ``web/cache.py``'s Redis client to the ``BeetsDistanceCache``
    protocol.

    The service-side keys already carry the ``youtube:album:`` /
    ``youtube:search:`` namespace; this adapter does NOT prefix them
    again (review finding #17 — the old ``_NAMESPACE`` wrapper produced
    ``youtube:album:youtube:album:<browse_id>`` keys).

    Mirrors ``_RedisFingerprintCache`` in ``web/routes/pipeline.py`` —
    bytes get/set with a long sentinel TTL (cache lives forever absent
    explicit refresh per Key Technical Decisions). Falls back to a
    no-op when Redis isn't available so the CLI works without the
    in-process accelerator.
    """

    def __init__(self) -> None:
        try:
            from web import cache as _cache_mod
            self._redis = getattr(_cache_mod, "_redis", None)
        except Exception:
            self._redis = None

    def get(self, key: str):
        if self._redis is None:
            return None
        try:
            raw = self._redis.get(key)  # type: ignore[union-attr]
        except Exception:
            return None
        if raw is None:
            return None
        # web/cache.py initialises Redis with ``decode_responses=True``,
        # so ``get`` returns str. Encode to bytes for the protocol.
        if isinstance(raw, str):
            return raw.encode("utf-8")
        return raw

    def set(self, key: str, value: bytes, ttl_seconds: int) -> None:
        if self._redis is None:
            return
        try:
            self._redis.setex(  # type: ignore[union-attr]
                key, ttl_seconds, value)
        except Exception:
            pass


def _build_youtube_client():
    """Construct a ``YTMusic`` client with retry + jittered desktop
    headers per the Key Technical Decisions (R5 / external research).

    Lazy-imports ``requests``, ``urllib3``, and ``ytmusicapi`` so the
    CLI's startup cost stays low and the rest of the script doesn't
    pay for unused HTTP machinery.

    Returns ``(yt_client, session)`` so the caller can close the
    session in a ``finally`` block — without that, every CLI
    invocation leaks the requests Session's connection pool. Round 2
    P2-2: the web-route side already paired finding #18's close in a
    ``finally``; this brings the CLI surface into parity per the
    CLI ⇄ API symmetry rule.

    The session binds a default ``(connect, read)`` timeout of
    ``(5, 30)`` so an unresponsive YT endpoint can't pin the CLI
    invocation forever (finding #4). ``requests`` exposes no
    Session-level timeout config; ``functools.partial`` on
    ``session.request`` is the established pattern.
    """
    from functools import partial
    import requests
    from urllib3.util.retry import Retry
    from requests.adapters import HTTPAdapter
    from ytmusicapi import YTMusic

    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    session.request = partial(  # type: ignore[method-assign]
        session.request, timeout=(5, 30))
    return YTMusic(requests_session=session, language="en"), session


def cmd_youtube_album(db, args):
    """``pipeline-cli youtube-album <identifier> [--refresh] [--json]``.

    Resolves any MB / Discogs release-or-group identifier into the
    YouTube Music distance matrix. Counterpart of ``GET
    /api/youtube-album`` (U8). Both surfaces wrap
    ``lib.youtube_album_service.resolve_youtube_album`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry"). The
    outcome → exit-code mapping is imported directly from the service
    module (``OUTCOME_EXIT_CODE``) to keep a single source of truth.

    Exit codes (from ``lib.youtube_album_service.OUTCOME_EXIT_CODE``):
      * 0 — ``ok``
      * 2 — ``not_found``
      * 5 — ``unresolved_4xx_client`` / ``unresolved_mirror_unavailable``
            / ``unresolved_timeout`` / ``youtube_parse_failed`` /
            ``transient``
      * 1 — unknown outcome (safety net)
    """
    from lib.beets_distance import compute_beets_distance
    from web import mb as mb_api
    from web import discogs as discogs_api

    yt, session = _build_youtube_client()
    cache = _RedisYoutubeCache()

    try:
        result = resolve_youtube_album(
            args.identifier,
            pdb=db,
            mb_get_release=lambda m: mb_api.get_release(m, fresh=False),
            mb_get_release_group_releases=mb_api.get_release_group_releases,
            discogs_get_release=lambda d: discogs_api.get_release(
                int(d), fresh=False),
            discogs_get_master_releases=lambda m: discogs_api.get_master_releases(
                int(m)),
            yt_client=yt,
            distance_fn=compute_beets_distance,
            cache=cache,
            refresh=bool(getattr(args, "refresh", False)),
        )
    finally:
        # Close the requests Session even when the resolver raises so
        # the connection pool doesn't leak. Mirrors the web-route side
        # (finding #18). Round 2 P2-2 — closes the CLI ⇄ API symmetry
        # gap.
        try:
            session.close()
        except Exception:
            pass

    if getattr(args, "json", False):
        print(msgspec.json.encode(result).decode())
    else:
        print(f"  identifier:             {args.identifier}")
        print(f"  outcome:                {result.outcome}")
        if result.release_group_identifier:
            print(f"  release group:          "
                  f"{result.release_group_identifier} ({result.source})")
        print(f"  from cache:             {result.from_cache}")
        if result.error_message:
            print(f"  error:                  {result.error_message}")
        if result.duration_ms is not None:
            print(f"  latency:                {result.duration_ms} ms")
        if result.youtube_releases:
            print(f"  matrix ({len(result.youtube_releases)} YT release(s)):")
            for yt_rel in result.youtube_releases:
                year = yt_rel.year if yt_rel.year is not None else "—"
                print(f"    - {yt_rel.yt_browse_id}  "
                      f"year={year}  tracks={yt_rel.track_count}")
                print(f"      url: {yt_rel.yt_url}")
                for d in yt_rel.distances:
                    if d.distance is not None:
                        dist_label = f"{d.distance:.4f}"
                    else:
                        dist_label = "n/a"
                    suffix = ""
                    if d.matched_tracks is not None \
                            and d.total_mb_tracks is not None:
                        suffix = (
                            f"  matched={d.matched_tracks}/"
                            f"{d.total_mb_tracks}")
                    err_suffix = (
                        f"  err={d.error_message}" if d.error_message else "")
                    print(f"      · {d.mbid}  outcome={d.outcome}  "
                          f"dist={dist_label}{suffix}{err_suffix}")
        else:
            # AE2 / R11 — empty matrix is a normal response, not an error.
            print("  matrix:                 (empty)")

    return OUTCOME_EXIT_CODE.get(result.outcome, 1)


def cmd_youtube_rescue(db, args, *, service_factory=None):
    """``pipeline-cli youtube-rescue <request_id> <browse_id> [--json]``.

    Submit a YouTube-Music rescue ingest for one album request. Counterpart
    of ``POST /api/pipeline/<id>/youtube-rescue`` (U5). Both surfaces wrap
    ``YoutubeIngestService.submit`` — keep them in sync (see ``CLAUDE.md``
    § "CLI ⇄ API surface symmetry"). The outcome → exit-code mapping is
    imported directly from the service module
    (``YOUTUBE_INGEST_EXIT_CODE``) to keep a single source of truth.

    Exit codes (from ``lib.youtube_ingest_service.OUTCOME_EXIT_CODE``):
      * 0 — ``accepted``
      * 2 — ``request_not_found``
      * 3 — ``no_resolver_mapping``, ``track_count_precheck_failed``
            (semantic input violations)
      * 4 — ``wrong_state`` (request is not ``wanted`` / ``manual``),
            ``in_flight`` (an existing ``youtube_running`` row already
            owns this request — re-issue once it's terminal)
      * 5 — ``transient`` (DB / MB-mirror hiccup; retry)
      * 1 — unknown outcome (safety net)
    """
    factory = service_factory or default_youtube_ingest_service_factory
    svc = factory(db)
    result = svc.submit(int(args.request_id), str(args.browse_id))

    if getattr(args, "json", False):
        print(msgspec.json.encode(result).decode())
    else:
        if result.outcome == "accepted":
            print(
                f"accepted: download_log_id={result.download_log_id}")
        else:
            # Failure paths print classified outcome + detail to stderr
            # so success-only consumers can pipe stdout without noise.
            sys.stderr.write(
                f"{result.outcome}"
                f"{f': {result.detail}' if result.detail else ''}\n"
            )
            if result.download_log_id is not None:
                # ``in_flight`` carries the existing log id; surface so
                # the operator knows where to look.
                sys.stderr.write(
                    f"  existing download_log_id={result.download_log_id}\n"
                )

    return YOUTUBE_INGEST_EXIT_CODE.get(result.outcome, 1)


def cmd_search_plan_dry_run(db, args):
    """U6: ``pipeline-cli search-plan dry-run <request_id>``.

    Read-only simulator: runs the current generator against the
    request's persisted snapshot and prints the slot list without
    writing anything. Counterpart of ``GET /api/pipeline/<id>/search-plan/dry-run``.
    Both surfaces wrap ``SearchPlanService.dry_run_for_request`` — keep
    them in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Use this during generator development (see
    ``.claude/rules/code-quality.md`` § "Pipeline Decision Debugging
    — Simulator-First TDD") to validate that the next cycle's
    generator output matches expectations before bumping
    ``SEARCH_PLAN_GENERATOR_ID``.

    Exit codes:
      * 0 — ``RESULT_DRY_RUN_SUCCESS`` or
        ``RESULT_DRY_RUN_GENERATION_FAILED`` (generator returned a
        deterministic generation failure — informational, not a CLI
        error; the operator still wants to see ``failure_reason`` and
        provenance).
      * 2 — ``RESULT_REQUEST_NOT_FOUND``.
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SearchPlanService,
        dry_run_payload,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.dry_run_for_request(
        int(args.id),
        prepend_artist=getattr(args, "prepend_artist", None),
    )
    row = db.get_request(int(args.id))
    has_active = False
    if row is not None:
        try:
            active = db.get_active_search_plan(int(args.id))
            has_active = active is not None
        except Exception:  # noqa: BLE001
            has_active = False
    payload = dry_run_payload(
        result,
        current_generator_id=svc.generator_id,
        request_row=row,
        has_active_plan=has_active,
    )

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:             {payload['request_id']}")
        print(f"  Outcome:                {payload['outcome']}")
        print(
            f"  Current generator id:   {payload['current_generator_id']}")
        if payload["request"] is not None:
            req = payload["request"]
            print(f"  Artist:                 {req.get('artist_name')}")
            print(f"  Album:                  {req.get('album_title')}")
            print(f"  Status:                 {req.get('status')}")
            print(f"  Year:                   {req.get('year') or '-'}")
            rg = req.get("release_group_year")
            print(f"  Release-group year:     {rg if rg is not None else '-'}")
            print(
                f"  Would supersede active: "
                f"{'yes' if payload['would_supersede_active'] else 'no'}")
        plan = payload["plan"]
        if plan is None:
            print(f"  Plan:                   (none)")
            if result.error_message:
                print(f"  Error message:          {result.error_message}")
        else:
            print(f"  Plan generator_id:      {plan['generator_id']}")
            print(f"  Plan status:            {plan['status']}")
            if plan["failure_reason"]:
                print(
                    f"  Plan failure_reason:    {plan['failure_reason']}")
            items = plan["items"]
            print(f"  Plan items ({len(items)}):")
            for it in items:
                head = (
                    f"    [{it['ordinal']:>2}] strategy={it['strategy']}"
                    f"  query={it['query']!r}")
                if it.get("canonical_query_key"):
                    head += f"  key={it['canonical_query_key']}"
                if it.get("repeat_group"):
                    head += f"  repeat={it['repeat_group']}"
                print(head)
                prov = it.get("provenance") or {}
                for key, value in prov.items():
                    print(f"          provenance.{key}: {value}")
            prov_plan = plan.get("provenance") or {}
            if prov_plan:
                print(f"  Plan provenance:")
                for key, value in prov_plan.items():
                    if isinstance(value, list):
                        print(f"    {key}: {len(value)} item(s)")
                        for entry in value[:5]:
                            print(f"      - {entry}")
                        if len(value) > 5:
                            print(f"      ... +{len(value) - 5} more")
                    else:
                        print(f"    {key}: {value}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_saturation(db, args):
    """U7: ``pipeline-cli search-plan saturation <request_id>``.

    Read-only telemetry aggregator: reports the saturation rate (rows
    whose ``final_state`` contains ``LimitReached``) and total
    ``pre_filter_skip_count`` over the last ``--window-days`` (default
    14) of ``search_log`` rows. Counterpart of
    ``GET /api/pipeline/<id>/search-plan/saturation``; both surfaces
    wrap ``SearchPlanService.saturation_for_request`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_SATURATION_SUCCESS`` (zeros are still success —
        the request exists, the window is just quiet)
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_SATURATION_INPUT_INVALID`` (argparse normally
        bounds this; the branch is defensive parity with the API's
        400)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SATURATION_WINDOW_DEFAULT_DAYS,
        SearchPlanService,
        saturation_payload,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    # ``None`` means "argparse default" (operator omitted the flag);
    # treat 0 / negative as explicit and let the service flag them
    # invalid so the operator sees the failure rather than silently
    # widening to 14.
    raw_window = getattr(args, "window_days", None)
    window_days = int(
        raw_window if raw_window is not None
        else SATURATION_WINDOW_DEFAULT_DAYS)
    result = svc.saturation_for_request(
        int(args.id), window_days=window_days,
    )
    payload = saturation_payload(result)

    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:             {payload['request_id']}")
        print(f"  Outcome:                {payload['outcome']}")
        print(f"  Window (days):          {payload['window_days']}")
        print(f"  Total searches:         {payload['total_searches']}")
        print(f"  Saturated searches:     {payload['saturated_searches']}")
        # Render the rate as a percentage with one decimal so the
        # number is human-readable at a glance.
        rate_pct = 100.0 * float(payload['saturation_rate'])
        print(f"  Saturation rate:        {rate_pct:.1f}%")
        print(
            f"  Pre-filter skips total: {payload['total_pre_filter_skips']}")
        if payload.get("error_message"):
            print(f"  Error message:          {payload['error_message']}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_advance(db, args):
    """Forward-only operator advance of the search-plan cursor.

    Counterpart of ``POST /api/pipeline/<id>/search-plan/advance``. Both
    surfaces wrap ``SearchPlanService.advance_for_request`` — keep them
    in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_ADVANCED``
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_INVALID_TARGET`` (out of range, would go backward,
        no slot matches strategy, or both/neither flag given)
      * 4 — ``RESULT_NO_ACTIVE_PLAN``
      * 5 — ``RESULT_FAILED_TRANSIENT`` (lock contention)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    result = svc.advance_for_request(
        int(args.id),
        to_ordinal=args.to_ordinal,
        to_strategy=args.to_strategy,
    )
    payload = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "plan_id": result.plan_id,
        "previous_ordinal": result.previous_ordinal,
        "new_ordinal": result.new_ordinal,
        "new_strategy": result.new_strategy,
        "new_query": result.new_query,
        "error_message": result.error_message,
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.plan_id is not None:
            print(f"  Plan id:           {result.plan_id}")
        if (result.previous_ordinal is not None
                and result.new_ordinal is not None):
            print(
                f"  Cursor:            {result.previous_ordinal} → "
                f"{result.new_ordinal}")
        if result.new_strategy is not None:
            print(f"  New slot strategy: {result.new_strategy}")
        if result.new_query is not None:
            print(f"  New slot query:    {result.new_query}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")

    return _search_plan_exit_code(result.outcome)


def cmd_search_plan_history(db, args):
    """Cursor-paginated read of one request's ``search_log`` rows.

    Counterpart of ``GET /api/pipeline/<id>/search-plan/history``. Both
    surfaces wrap ``SearchPlanService.history_for_request`` — keep them
    in sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Default limit is ``HISTORY_PAGE_DEFAULT_LIMIT`` (50). Pass
    ``--before-id <id>`` to read the next page. JSON mode returns the
    same payload as the API (``request_id`` / ``rows`` / ``next_before_id``).

    Exit codes:
      * 0 — ``RESULT_HISTORY_PAGE_SUCCESS``
      * 2 — ``RESULT_REQUEST_NOT_FOUND``
      * 3 — ``RESULT_HISTORY_PAGE_INPUT_INVALID`` (limit out of bounds,
        before_id < 1)
    """
    from lib.config import read_runtime_config
    from lib.search_plan_service import (
        HISTORY_PAGE_DEFAULT_LIMIT,
        RESULT_HISTORY_PAGE_SUCCESS,
        SearchPlanService,
    )

    cfg = read_runtime_config()
    svc = SearchPlanService(db, cfg)
    limit = args.limit if args.limit is not None else HISTORY_PAGE_DEFAULT_LIMIT
    result = svc.history_for_request(
        int(args.id),
        limit=int(limit),
        before_id=args.before_id,
    )
    payload = {
        "request_id": result.request_id,
        "rows": result.rows,
        "next_before_id": result.next_before_id,
        "outcome": result.outcome,
        "error_message": result.error_message,
    }
    if getattr(args, "json", False):
        if result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
            # F7: strip internal routing keys so --json output matches the
            # API 200 shape (CLI ⇄ API surface symmetry, CLAUDE.md).
            api_payload = {
                "request_id": result.request_id,
                "rows": result.rows,
                "next_before_id": result.next_before_id,
            }
        else:
            api_payload = payload
        print(json.dumps(api_payload, indent=2, sort_keys=True,
                         default=_json_default))
    elif result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
        print(f"  Request ID:        {result.request_id}")
        print(f"  Rows on page:      {len(result.rows)}")
        print(f"  Next before-id:    "
              f"{result.next_before_id if result.next_before_id is not None else '-'}")
        for row in result.rows:
            created = row.get("created_at") or "-"
            outcome = row.get("outcome") or "-"
            strategy = row.get("plan_strategy") or "(legacy)"
            ordinal = row.get("plan_ordinal")
            ord_str = f"ord={ordinal}" if ordinal is not None else "ord=-"
            query = row.get("query") or ""
            row_id = row.get("id")
            print(
                f"  [{created}] id={row_id} {outcome} {strategy} "
                f"{ord_str} query={query!r}"
            )
        if result.next_before_id is not None:
            print(
                "  Next page: "
                f"pipeline-cli search-plan history {result.request_id} "
                f"--before-id {result.next_before_id}"
            )
    else:
        print(f"  Request ID:        {result.request_id}")
        print(f"  Outcome:           {result.outcome}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")

    return _search_plan_exit_code(result.outcome)


# --- U16: pipeline-cli triage ------------------------------------------------
#
# Two subcommands wrap the U15 triage service:
#
#   * ``pipeline-cli triage show <id>`` — per-request composition.
#   * ``pipeline-cli triage list --filter=<spec>`` — cohort listing.
#
# Both adhere to CLAUDE.md § "CLI ⇄ API surface symmetry": each one is a
# thin wrapper around ``lib.triage_service``; the matching HTTP routes
# (U17) wrap the same service with the same outcome → exit-code /
# status-code mapping.


# The canonical machine-parseable forms come from the service-layer
# ``VALID_FILTER_FORMS`` (single source of truth across CLI and HTTP);
# the prose variants below are CLI-only embellishments to help operators
# remember the parameterised vocab. New filter forms get added at the
# service layer; both wrappers auto-pick them up.
from lib.triage_service import VALID_FILTER_FORMS as _TRIAGE_VALID_FILTER_FORMS_BASE  # noqa: E402

_TRIAGE_VALID_FILTER_FORMS = (
    "all",
    "unfindable",
    "unfindable:<category>  (category ∈ "
    "{artist_absent, album_absent_artist_present, "
    "one_track_structural, wrong_pressing_available})",
    "data_quality",
    "data_quality:<field_name>  (field ∈ "
    "{release_group_year, release_group_id, track_artist, catalog_number})",
    "data_quality:status=<resolver_status>  (e.g. "
    "unresolved_4xx_client, unresolved_404, unresolved_timeout)",
    "data_quality:reason=<reason_code>  (e.g. http_400, http_410, "
    "http_422)",
    "search_not_converting",
)


def _truncate(text: str, width: int) -> str:
    """Truncate ``text`` to ``width`` characters, marking with an ellipsis.

    Pure helper used by ``triage list``'s human-readable table renderer.
    Avoids pulling in textwrap for a 4-line helper.
    """
    if len(text) <= width:
        return text
    if width <= 1:
        return text[:width]
    return text[: width - 1] + "…"


def _format_dt(value: object) -> str:
    """Compact display of a datetime/date/time for table cells.

    Uses the same ISO 8601 rendering ``_json_default`` emits, but strips
    sub-second precision so the table stays narrow. Returns ``"-"`` on
    ``None`` so empty cells are visually distinct from a zero-length
    string. ``object`` is wider than necessary, but the helper is used
    against ``msgspec.to_builtins`` output which is statically untyped.
    """
    if value is None:
        return "-"
    if isinstance(value, (date, datetime, time)):
        iso = value.isoformat()
        # Drop microseconds + timezone marker for table compactness.
        if "." in iso:
            iso = iso.split(".", 1)[0]
        if iso.endswith("+00:00"):
            iso = iso[:-6] + "Z"
        return iso
    return str(value)


def cmd_triage_show(db, args):
    """``pipeline-cli triage show <id>`` — per-request triage composition.

    Wraps ``compose_triage_for_request`` and renders the full payload
    (request meta + unfindable + field-quality + search forensics +
    recent search_log slice). Mirrors the human/JSON conventions
    established by ``cmd_search_plan_show`` and the U17 API.

    Exit codes:
      * 0 — success
      * 2 — request not found
    """
    from lib.triage_service import compose_triage_for_request

    rid = int(args.id)
    result = compose_triage_for_request(rid, db)
    if result is None:
        if getattr(args, "json", False):
            print(json.dumps({
                "error": "Not found",
                "request_id": rid,
            }, indent=2, sort_keys=True))
        else:
            print(f"  Request {rid} not found.", file=sys.stderr)
        return 2

    if getattr(args, "json", False):
        payload = msgspec.to_builtins(result)
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    # Human-readable rendering.
    meta = result.request_meta
    print(f"  Request ID:        {meta.id}")
    print(f"  Artist / Album:    {meta.artist_name} — {meta.album_title}")
    if meta.year is not None:
        print(f"  Year:              {meta.year}")
    print(f"  Status:            {meta.status}")
    print(f"  Source:            {meta.source or '-'}")
    if meta.mb_release_id:
        print(f"  MB release id:     {meta.mb_release_id}")
    if meta.discogs_release_id:
        print(f"  Discogs id:        {meta.discogs_release_id}")
    if meta.failure_class:
        print(f"  Failure class:     {meta.failure_class}")
    if meta.search_filetype_override:
        print(f"  Search filetype:   {meta.search_filetype_override}")

    # Unfindable cohort state.
    if result.unfindable is None:
        print("  Unfindable:        (no signals)")
    else:
        u = result.unfindable
        print("  Unfindable:")
        print(f"    category:                  {u.category or '-'}")
        print(f"    categorised_at:            {_format_dt(u.categorised_at)}")
        print(
            f"    last_artist_probe_at:      "
            f"{_format_dt(u.last_artist_probe_at)}"
        )
        if u.last_artist_probe_match_count is not None:
            print(
                f"    last_probe_match_count:    "
                f"{u.last_artist_probe_match_count}"
            )
        if u.rescued_at is not None:
            print(f"    rescued_at:                {_format_dt(u.rescued_at)}")
            print(
                f"    prior_unfindable_category: "
                f"{u.prior_unfindable_category or '-'}"
            )

    # Field-quality rows (the resolver side table, U2).
    if not result.field_quality:
        print("  Field quality:     (no resolutions)")
    else:
        print(f"  Field quality:     {len(result.field_quality)} resolution(s)")
        for fr in result.field_quality:
            reason = fr.reason_code or "-"
            print(
                f"    [{fr.field_name}] status={fr.status} reason={reason} "
                f"attempts={fr.attempts} resolved_at={_format_dt(fr.resolved_at)}"
            )

    # Search forensics summary + recent entries.
    sf = result.search_forensics
    print("  Search forensics:")
    print(f"    total_searches:            {sf.total_searches}")
    print(f"    with_cands_count:          {sf.with_cands_count}")
    print(f"    found_count:               {sf.found_count}")
    print(f"    near_cap_count:            {sf.near_cap_count}")
    print(f"    zero_results_count:        {sf.zero_results_count}")
    print(f"    pre_filter_skips_total:    {sf.pre_filter_skips_total}")
    print(
        f"    first_strategy_with_cands: "
        f"{sf.first_strategy_with_cands or '-'}"
    )
    print(
        f"    dominant_rejection_reason: "
        f"{sf.dominant_rejection_reason or '-'}"
    )
    print(f"    last_search_at:            {_format_dt(sf.last_search_at)}")

    if not sf.recent_entries:
        print("    recent_entries:            (none)")
    else:
        print(f"    recent_entries:            {len(sf.recent_entries)} "
              f"(newest first, max 10)")
        for entry in sf.recent_entries:
            strategy = entry.plan_strategy or "(legacy)"
            reason = entry.rejection_reason or "-"
            matcher = (
                f"{entry.matcher_score_top1:.2f}"
                if entry.matcher_score_top1 is not None else "-"
            )
            rc = (
                str(entry.result_count) if entry.result_count is not None
                else "-"
            )
            query = entry.query or ""
            print(
                f"      [{_format_dt(entry.created_at)}] id={entry.id} "
                f"{entry.outcome} {strategy} rc={rc} reject={reason} "
                f"matcher={matcher} query={query!r}"
            )

    return 0


def cmd_triage_list(db, args):
    """``pipeline-cli triage list --filter=<spec>`` — cohort listing.

    Wraps ``list_triage``. Default page size is 50. Use ``--after=<id>``
    to resume; the page footer prints the next ``--after`` value when
    the returned page is exactly ``--limit`` long.

    Exit codes:
      * 0 — success (empty list is a valid cohort state)
      * 3 — invalid filter spec (``InvalidFilterError``) or out-of-range
        ``--limit`` / ``--after``

    JSON envelope (mirrors the API):
        ``{"results": [...], "next_after": <int|null>,
           "page_size": <int>, "filter": <spec>}``
    """
    from lib.triage_service import (
        InvalidFilterError,
        TRIAGE_AFTER_MIN,
        TRIAGE_LIMIT_MAX,
        TRIAGE_LIMIT_MIN,
        list_triage,
    )

    json_mode = bool(getattr(args, "json", False))
    limit = int(args.limit) if args.limit is not None else 50
    after = int(args.after) if args.after is not None else None

    # Bounds — mirrors the API's [1..200] / [>=1] check so the two
    # surfaces reject the same set of out-of-range values.
    if not (TRIAGE_LIMIT_MIN <= limit <= TRIAGE_LIMIT_MAX):
        msg = (
            f"--limit must be in [{TRIAGE_LIMIT_MIN}, {TRIAGE_LIMIT_MAX}]; "
            f"got {limit}"
        )
        if json_mode:
            print(json.dumps({"error": msg}, indent=2, sort_keys=True))
        else:
            print(msg, file=sys.stderr)
        return 3
    if after is not None and after < TRIAGE_AFTER_MIN:
        msg = f"--after must be >= {TRIAGE_AFTER_MIN}; got {after}"
        if json_mode:
            print(json.dumps({"error": msg}, indent=2, sort_keys=True))
        else:
            print(msg, file=sys.stderr)
        return 3

    try:
        results = list_triage(
            args.filter, db,
            page_size=limit,
            after_request_id=after,
        )
    except InvalidFilterError as exc:
        from lib.triage_service import (
            VALID_DATA_QUALITY_FIELD_NAMES,
            VALID_UNFINDABLE_CATEGORIES,
        )
        if json_mode:
            # JSON-mode error path — emit a structured payload on stdout
            # so callers piping ``--json | jq`` keep parsing. Mirrors
            # cmd_triage_show's 404 JSON path and the API 400 envelope.
            print(json.dumps(
                {
                    "error": str(exc),
                    "valid_filters": list(_TRIAGE_VALID_FILTER_FORMS_BASE),
                    "valid_unfindable_categories": sorted(
                        VALID_UNFINDABLE_CATEGORIES
                    ),
                    "valid_data_quality_fields": sorted(
                        VALID_DATA_QUALITY_FIELD_NAMES
                    ),
                },
                indent=2, sort_keys=True,
            ))
        else:
            message = (
                f"Invalid filter spec: {exc}\n"
                "Valid forms:\n"
                + "\n".join(f"  - {form}" for form in _TRIAGE_VALID_FILTER_FORMS)
            )
            print(message, file=sys.stderr)
        return 3

    # ``next_after`` matches the API's ``>= limit`` predicate so the
    # CLI and HTTP surfaces report identical pagination state on the
    # same data.
    next_after: int | None = None
    if results and len(results) >= limit:
        next_after = results[-1].request_meta.id

    if json_mode:
        # Envelope wrap matches the API shape so agents pipe-and-jq the
        # same way against both surfaces.
        payload = {
            "results": msgspec.to_builtins(results),
            "next_after": next_after,
            "page_size": limit,
            "filter": args.filter,
        }
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    if not results:
        print(f"  No results for filter={args.filter!r}.")
        return 0

    # Human table.
    header_cols = (
        ("id", 6),
        ("artist", 25),
        ("album", 25),
        ("status", 12),
        ("category/failure", 28),
        ("last_search_at", 20),
    )
    header_line = "  ".join(name.ljust(width) for name, width in header_cols)
    print(header_line)
    print("  ".join("-" * width for _, width in header_cols))

    for r in results:
        meta = r.request_meta
        category_or_failure = (
            (r.unfindable.category if r.unfindable is not None else None)
            or meta.failure_class
            or "-"
        )
        last_search = _format_dt(r.search_forensics.last_search_at)
        row_cells = (
            str(meta.id),
            _truncate(meta.artist_name, 25),
            _truncate(meta.album_title, 25),
            _truncate(meta.status, 12),
            _truncate(category_or_failure, 28),
            _truncate(last_search, 20),
        )
        print("  ".join(
            cell.ljust(width) for cell, (_, width) in zip(row_cells, header_cols)
        ))

    print(f"  ({len(results)} rows)")
    if next_after is not None:
        print(
            f"  next page: pipeline-cli triage list --filter={args.filter} "
            f"--limit={limit} --after={next_after}"
        )
    return 0


def _cli_band_fn(release_ids):
    """Build the long-tail band map for the CLI.

    Reuses the SAME banding decision the web overlay uses
    (``web.routes._overlay._band_from_detail`` →
    ``web.server.compute_library_rank``) but sources beets membership /
    detail from a directly-opened ``BeetsDB`` rather than the web
    server's module-level ``_beets`` global (which the CLI process never
    sets). No parallel banding logic — only the beets-access seam
    differs between the two surfaces.

    Returns ``{release_id: band}`` (``"missing"`` / a lowercase
    ``QualityRank`` / ``"unknown"``). Best-effort: if beets is
    unreachable every id bands ``"missing"`` (no on-disk copy to upgrade
    is the honest fallback).
    """
    from lib.beets_db import BeetsDB
    from web.routes._overlay import _band_from_detail

    ids_list = [str(rid) for rid in release_ids]
    if not ids_list:
        return {}
    try:
        with BeetsDB() as beets:
            in_library = beets.check_mbids(ids_list)
            quality = (
                beets.check_mbids_detail(list(in_library))
                if in_library else {}
            )
    except Exception:
        return {rid: "missing" for rid in ids_list}
    return {
        rid: _band_from_detail(rid, in_library, quality) for rid in ids_list
    }


def cmd_long_tail(db, args, *, band_fn=None):
    """``pipeline-cli long-tail [--band=<band>] [--json]``.

    The long-tail worklist read — every ``wanted`` request pre-banded by
    on-disk quality (``missing`` / a lowercase ``QualityRank`` band /
    ``unknown``) and stamped with ``in_flight_rescue``. Counterpart of
    ``GET /api/pipeline/long-tail`` (U1). Both surfaces wrap
    ``lib.long_tail_service.list_long_tail`` — keep them in sync
    (CLI ⇄ API symmetry).

    ``--id`` requests a single banded row (KTD8 — the post-action
    refetch counterpart of ``GET /api/pipeline/long-tail?id=``); exits 2
    when the id doesn't exist or is no longer ``wanted``.

    ``band_fn`` is a kwarg-DI seam (defaults to ``_cli_band_fn``, the
    real BeetsDB-backed banding); tests inject a deterministic fake so
    they don't need a live beets library.

    Exit codes:
      * 0 — success (empty cohort is a valid state)
      * 2 — ``--id`` not found / not ``wanted``

    JSON envelope (mirrors the API):
        ``{"results": [...], "band": <str|null>, "count": <int>}``
    Single-id JSON (mirrors the API):
        ``{"result": <row>, "id": <int>}``
    """
    from lib.long_tail_service import band_one_long_tail, list_long_tail

    json_mode = bool(getattr(args, "json", False))
    resolved_band_fn = band_fn if band_fn is not None else _cli_band_fn

    request_id = getattr(args, "id", None)
    if request_id is not None:
        row = band_one_long_tail(db, resolved_band_fn, int(request_id))
        if row is None:
            msg = f"request {int(request_id)} not found or not wanted"
            if json_mode:
                print(json.dumps(
                    {"error": "Not found", "id": int(request_id)},
                    indent=2, sort_keys=True))
            else:
                print(msg, file=sys.stderr)
            return 2
        if json_mode:
            print(json.dumps(
                {"result": msgspec.to_builtins(row), "id": int(request_id)},
                indent=2, sort_keys=True, default=_json_default))
        else:
            print(f"  [{row.id}] {row.artist_name} - {row.album_title}")
            print(f"  band:            {row.band}")
            print(f"  in_flight_rescue: {row.in_flight_rescue}")
        return 0

    band = getattr(args, "band", None)
    if band == "":
        band = None

    result = list_long_tail(db, resolved_band_fn, band=band)

    if json_mode:
        payload = {
            "results": msgspec.to_builtins(result.rows),
            "band": result.band_filter,
            "count": len(result.rows),
        }
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
        return 0

    if not result.rows:
        suffix = f" for band={band!r}" if band else ""
        print(f"  No wanted rows{suffix}.")
        return 0

    header_cols = (
        ("id", 6),
        ("artist", 25),
        ("album", 25),
        ("band", 12),
        ("rescue", 7),
        ("category", 22),
    )
    print("  ".join(name.ljust(width) for name, width in header_cols))
    print("  ".join("-" * width for _, width in header_cols))
    for r in result.rows:
        row_cells = (
            str(r.id),
            _truncate(r.artist_name, 25),
            _truncate(r.album_title, 25),
            _truncate(r.band, 12),
            "yes" if r.in_flight_rescue else "-",
            _truncate(r.unfindable_category or "-", 22),
        )
        print("  ".join(
            cell.ljust(width) for cell, (_, width) in zip(row_cells, header_cols)
        ))
    print(f"  ({len(result.rows)} rows)")
    return 0


def _build_parser() -> tuple[
    argparse.ArgumentParser, argparse.ArgumentParser, argparse.ArgumentParser
]:
    """Build the full pipeline-cli argument parser.

    Returned tuple is ``(top_level, search_plan_subparser,
    triage_subparser)``; ``main()`` uses the nested subparsers to print
    helpful errors when an operator runs ``search-plan`` / ``triage``
    without a subcommand, and ``cmd_routes`` uses the top-level parser
    to introspect every registered subcommand.
    """
    parser = argparse.ArgumentParser(description="Pipeline CLI — manage download pipeline DB")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    sub = parser.add_subparsers(dest="command")

    # list
    p_list = sub.add_parser("list", help="List album requests")
    p_list.add_argument("filter_status", nargs="?", help="Filter by status")

    # add
    p_add = sub.add_parser("add", help="Add a new request by MBID or Discogs ID")
    p_add.add_argument("mbid", help="MusicBrainz release UUID or Discogs numeric release ID")
    p_add.add_argument("--source", default="request", choices=["request", "redownload", "manual"],
                       help="Source type (default: request)")

    # query
    p_query = sub.add_parser("query", help="Run a read-only SQL query for debugging")
    p_query.add_argument("sql", help="SQL query string, or '-' to read SQL from stdin")
    p_query.add_argument("--json", action="store_true", help="Print rows as JSON")

    # status
    sub.add_parser("status", help="Show counts by status")

    # disk-coverage
    p_disk = sub.add_parser(
        "disk-coverage",
        help="Show which active pipeline rows are actually present in beets",
    )
    p_disk.add_argument(
        "--beets-db",
        default=os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db"),
        help="Path to beets SQLite DB (default: BEETS_DB or production path)",
    )
    p_disk.add_argument(
        "--counts-only",
        action="store_true",
        help="Suppress the off-disk row list and print counts only",
    )
    p_disk.add_argument(
        "--include-inverse",
        action="store_true",
        help="Also include beets albums with no active pipeline row",
    )

    # retry
    p_retry = sub.add_parser("retry", help="Reset a failed request to wanted")
    p_retry.add_argument("id", type=int, help="Request ID")

    # cancel
    p_cancel = sub.add_parser("cancel", help="Cancel a request (set to skipped)")
    p_cancel.add_argument("id", type=int, help="Request ID")

    # set
    p_set = sub.add_parser("set", help="Change the status of a request")
    p_set.add_argument("id", type=int, help="Request ID")
    p_set.add_argument("status", choices=VALID_STATUSES, help="New status")

    # show
    p_show = sub.add_parser("show", help="Show full details of a request")
    p_show.add_argument("id", type=int, help="Request ID")

    # search-plan
    p_sp = sub.add_parser(
        "search-plan",
        help="Inspect persisted search plans (read-only, U6)")
    sp_sub = p_sp.add_subparsers(dest="search_plan_command")
    p_sp_show = sp_sub.add_parser(
        "show",
        help="Show active/failed plans, cursor, items, provenance, "
             "legacy logs for one request")
    p_sp_show.add_argument("id", type=int, help="Request ID")
    p_sp_show.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")
    p_sp_show.add_argument("--no-stats", action="store_true",
                            dest="no_stats",
                            help="Suppress per-slot/query usefulness stats")
    p_sp_regen = sp_sub.add_parser(
        "regenerate",
        help="Regenerate the search plan for a request (U8)")
    p_sp_regen.add_argument("id", type=int, help="Request ID")
    p_sp_regen.add_argument("--prepend-artist", action="store_true",
                             dest="prepend_artist", default=None,
                             help="Prepend artist name to album title in "
                             "generated queries (overrides config; absent "
                             "means use config's album_prepend_artist)")
    p_sp_regen.add_argument("--json", action="store_true",
                             help="Print structured JSON instead of text")
    p_sp_advance = sp_sub.add_parser(
        "advance",
        help="Forward-only operator advance of the cursor (e.g. skip "
             "collapsed default-strategy slots on a self-titled release)")
    p_sp_advance.add_argument("id", type=int, help="Request ID")
    sp_target = p_sp_advance.add_mutually_exclusive_group(required=True)
    sp_target.add_argument(
        "--to-ordinal", type=int, dest="to_ordinal",
        help="Absolute target ordinal in [0, plan_item_count)")
    sp_target.add_argument(
        "--to-strategy", dest="to_strategy",
        help="Strategy prefix; advance to the first plan item past the "
             "current cursor whose strategy starts with this string "
             "(e.g. `track`, `unwild_year`)")
    p_sp_advance.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_dry_run = sp_sub.add_parser(
        "dry-run",
        help="Run the generator against the request's snapshot without "
             "persisting (U6 simulator)")
    p_sp_dry_run.add_argument("id", type=int, help="Request ID")
    p_sp_dry_run.add_argument("--prepend-artist", action="store_true",
                              dest="prepend_artist", default=None,
                              help="Prepend artist name to album title in "
                              "generated queries (overrides config; absent "
                              "means use config's album_prepend_artist)")
    p_sp_dry_run.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")
    p_sp_saturation = sp_sub.add_parser(
        "saturation",
        help="Show per-request saturation rate + pre-filter skip total "
             "over the recent search_log window (U7 telemetry)")
    p_sp_saturation.add_argument("id", type=int, help="Request ID")
    p_sp_saturation.add_argument(
        "--window-days", type=int, default=None, dest="window_days",
        help="Window in days; defaults to 14; valid range [1, 90]")
    p_sp_saturation.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of text")
    p_sp_history = sp_sub.add_parser(
        "history",
        help="Cursor-paginated read of one request's search_log rows "
             "(per-attempt forensics)")
    p_sp_history.add_argument("id", type=int, help="Request ID")
    p_sp_history.add_argument(
        "--limit", type=int, default=None,
        help="Rows per page; defaults to 50; valid range [1, 200]")
    p_sp_history.add_argument(
        "--before-id", type=int, default=None, dest="before_id",
        help="Resume cursor: pass the previous page's next_before_id")
    p_sp_history.add_argument("--json", action="store_true",
                              help="Print structured JSON instead of text")

    # triage (U16) — operator-facing composition of unfindable + field-quality
    # + search-forensics. Wraps ``lib.triage_service`` (U15). Nested under a
    # subparser for the same reason ``search-plan`` is: the per-request view
    # and the cohort list share enough state to benefit from a shared
    # namespace, and the convention is consistent with the rest of this CLI.
    p_triage_op = sub.add_parser(
        "triage",
        help="Operator triage (U16) — compose unfindable + field-quality + "
             "search-forensics for one request, or list a cohort by filter")
    tr_sub = p_triage_op.add_subparsers(dest="triage_command")

    p_tr_show = tr_sub.add_parser(
        "show",
        help="Per-request triage composition (request meta + unfindable + "
             "field-quality + search forensics + last 10 search_log rows). "
             "Note: subcommand form mirrors `search-plan show <id>`; bare "
             "`triage <id>` is not accepted.")
    p_tr_show.add_argument("id", type=int, help="Request ID")
    p_tr_show.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")

    p_tr_list = tr_sub.add_parser(
        "list",
        help="Cohort listing by filter spec")
    p_tr_list.add_argument(
        "--filter", default="all",
        help="Filter spec: all | unfindable[:<category>] | "
             "data_quality[:<field>] | data_quality:status=<status> | "
             "data_quality:reason=<code> | search_not_converting")
    p_tr_list.add_argument(
        "--limit", type=int, default=50,
        help="Page size (default 50)")
    p_tr_list.add_argument(
        "--after", type=int, default=None,
        help="Resume cursor: last request_id from prior page")
    p_tr_list.add_argument("--json", action="store_true",
                            help="Print structured JSON instead of text")

    # long-tail
    p_long_tail = sub.add_parser(
        "long-tail",
        help="Long-tail worklist — wanted cohort pre-banded by on-disk "
             "quality (missing / QualityRank / unknown) + in_flight_rescue")
    p_long_tail.add_argument(
        "--band", default=None,
        help="Filter to a single band: missing | transparent | excellent "
             "| good | acceptable | poor | unknown")
    p_long_tail.add_argument(
        "--id", type=int, default=None,
        help="Band a single request by id (post-action refetch); "
             "exits 2 if not found / not wanted")
    p_long_tail.add_argument("--json", action="store_true",
                             help="Print structured JSON instead of text")

    # quality
    p_quality = sub.add_parser("quality", help="Show quality state and simulate decisions")
    p_quality.add_argument("id", type=int, help="Request ID")

    # set-intent
    p_intent = sub.add_parser("set-intent", help="Toggle lossless-on-disk for a request")
    p_intent.add_argument("id", type=int, help="Request ID")
    p_intent.add_argument("intent", choices=["lossless", "default"],
                          help="'lossless' = keep lossless on disk, 'default' = pipeline decides")

    # force-import
    p_force = sub.add_parser("force-import", help="Force-import a rejected download by download_log ID")
    p_force.add_argument("download_log_id", type=int, help="Download log ID")
    p_force.add_argument("--verified-lossless-target",
                         help="Override the runtime verified-lossless target for this import")

    # manual-import
    p_manual = sub.add_parser("manual-import", help="Import a local folder as a pipeline request")
    p_manual.add_argument("id", type=int, help="Pipeline request ID")
    p_manual.add_argument("path", help="Path to album folder")
    p_manual.add_argument("--verified-lossless-target",
                          help="Override the runtime verified-lossless target for this import")

    # import-jobs
    p_jobs = sub.add_parser("import-jobs", help="List recent import queue jobs")
    p_jobs.add_argument("--status", choices=["queued", "running", "completed", "failed"])
    p_jobs.add_argument("--limit", type=int, default=20)

    # import-preview
    p_preview = sub.add_parser("import-preview", help="Preview whether an import would pass")
    p_preview.add_argument("--download-log-id", type=int,
                           help="Preview the failed_path from a download_log row")
    p_preview.add_argument("--request-id", type=int,
                           help="Request ID for --path preview")
    p_preview.add_argument("--path", help="Preview a real folder for a request")
    p_preview.add_argument("--source-username",
                           help="Source username for preview audit context")
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
    p_preview.add_argument("--import-mode")
    p_preview.add_argument("--has-nested-audio", action="store_true", default=None)

    # wrong-match-triage
    p_triage = sub.add_parser(
        "wrong-match-triage",
        help="Clean the full Wrong Matches queue using existing evidence",
    )
    p_triage.add_argument("--apply", action="store_true",
                          help="Allow destructive full-queue cleanup")
    p_triage.add_argument("--json", action="store_true")

    # wrong-match-delete
    p_wm_delete = sub.add_parser(
        "wrong-match-delete",
        help="Delete one visible Wrong Matches source folder",
    )
    p_wm_delete.add_argument("download_log_id", type=int)
    p_wm_delete.add_argument("--apply", action="store_true",
                             help="Allow destructive source deletion")
    p_wm_delete.add_argument("--json", action="store_true")

    # wrong-match-delete-group
    p_wm_delete_group = sub.add_parser(
        "wrong-match-delete-group",
        help="Delete visible Wrong Matches source folders for one request",
    )
    p_wm_delete_group.add_argument("request_id", type=int)
    p_wm_delete_group.add_argument("--apply", action="store_true",
                                   help="Allow destructive source deletion")
    p_wm_delete_group.add_argument("--json", action="store_true")

    # repair-spectral
    p_repair = sub.add_parser("repair-spectral",
                              help="Fix albums stuck by stale current_spectral_bitrate (#18)")
    p_repair.add_argument("--dry-run", action="store_true",
                          help="Show what would be repaired without changing anything")

    # replace
    p_replace = sub.add_parser(
        "replace",
        help="Supersede a request with a new row at a different MBID "
             "in the same release group")
    p_replace.add_argument("id", type=int, help="Source request ID")
    p_replace.add_argument(
        "--to", dest="target_mb_release_id", required=True,
        help="Target MB release ID (must share the source's release group)")
    p_replace.add_argument("--json", action="store_true",
                           help="Print structured JSON instead of text")

    # beets-distance
    p_bd = sub.add_parser(
        "beets-distance",
        help="Real beets-distance between a download_log's audio and an MBID "
             "(refuses if MBID is outside the request's release group)")
    p_bd.add_argument("download_log_id", type=int,
                      help="download_log row id (see `pipeline-cli show <req>`)")
    p_bd.add_argument("mbid",
                      help="Candidate MB release UUID")
    p_bd.add_argument("--json", action="store_true",
                      help="Print structured JSON instead of text")

    # youtube-album (U7): MBID/Discogs ID → YT Music album matrix.
    # Counterpart of ``GET /api/youtube-album`` (U8).
    p_ya = sub.add_parser(
        "youtube-album",
        help="Resolve MBID/Discogs ID → YouTube Music album matrix "
             "(auto-widens to release group; N×M beets distances per "
             "YT sibling × MB sibling)",
    )
    p_ya.add_argument(
        "identifier",
        help="MB release/release-group MBID OR Discogs release/master ID "
             "(service auto-discriminates via leaf-then-group fallback)",
    )
    p_ya.add_argument(
        "--refresh", action="store_true",
        help="Bypass BOTH the durable cache (youtube_album_mappings) "
             "AND the in-process Redis HTTP accelerator, forcing a "
             "fresh YouTube Music fetch. The fresh response is then "
             "written back to both layers. (Default: serve from cache.)",
    )
    p_ya.add_argument(
        "--json", action="store_true",
        help="Print structured JSON instead of human-readable matrix",
    )

    # youtube-rescue (U4): submit a YouTube Music rescue ingest for one
    # request. Counterpart of ``POST /api/pipeline/<id>/youtube-rescue``
    # (U5). Both surfaces wrap ``YoutubeIngestService.submit``.
    p_yr = sub.add_parser(
        "youtube-rescue",
        help="Submit a YouTube Music rescue ingest for one request "
             "(requires a resolver mapping; emits a youtube_running "
             "download_log row).",
    )
    p_yr.add_argument(
        "request_id", type=int,
        help="album_requests.id to attach the rescue to",
    )
    p_yr.add_argument(
        "browse_id",
        help="YouTube Music browse_id (e.g. MPREb_...); must already "
             "be cached in youtube_album_mappings for this request's "
             "release group",
    )
    p_yr.add_argument(
        "--json", action="store_true",
        help="Print structured JSON ({outcome, download_log_id, detail}) "
             "instead of plain text.",
    )

    # routes (U18 step 3): self-document the CLI surface. Mirrors
    # ``GET /api/_index`` on the web side; both are read-only and zero-arg.
    p_routes = sub.add_parser(
        "routes",
        help="Self-document the CLI surface — every subcommand, "
             "its args, and its description.",
    )
    p_routes.add_argument(
        "--json", action="store_true",
        help="Emit JSON instead of human-readable text.",
    )

    return parser, p_sp, p_triage_op


def _describe_argparse_action(
    action: argparse.Action,
) -> str | None:
    """Render one argparse Action as a human-readable arg label.

    Returns the metavar / option-string form with a hint for type or
    choices when present. Returns None for the subparsers placeholder
    (those are sibling subcommands, not arguments).
    """
    if isinstance(action, argparse._SubParsersAction):  # noqa: SLF001
        return None
    if isinstance(action, argparse._HelpAction):  # noqa: SLF001
        return None
    label: str
    if action.option_strings:
        label = action.option_strings[0]
    else:
        metavar = action.metavar
        # ``metavar`` is typed ``str | tuple[str, ...] | None`` upstream;
        # nargs forms like ``nargs="?"`` can yield a tuple here. Render
        # tuples by joining for a stable string representation.
        if isinstance(metavar, tuple):
            label = " ".join(str(m) for m in metavar)
        else:
            label = metavar or action.dest
    type_hint: str | None = None
    if action.choices:
        type_hint = "{" + ",".join(str(c) for c in action.choices) + "}"
    elif action.type is int:
        type_hint = "int"
    elif action.type is float:
        type_hint = "float"
    if type_hint:
        return f"{label} ({type_hint})"
    return label


def _collect_cli_routes(
    parser: argparse.ArgumentParser,
) -> list[dict[str, object]]:
    """Walk a parser's subparsers and emit one entry per leaf subcommand.

    Returns a list of ``{subcommand, args, description}`` rows sorted by
    ``subcommand``. Nested subparsers (e.g. ``search-plan show``) emit
    one row per leaf path; the parent ``search-plan`` itself is not
    emitted because its help is already covered by its children.
    """
    rows: list[dict[str, object]] = []

    def _walk(p: argparse.ArgumentParser, prefix: str) -> None:
        sub_actions = [
            a for a in p._actions  # noqa: SLF001
            if isinstance(a, argparse._SubParsersAction)  # noqa: SLF001
        ]
        if not sub_actions:
            return
        for sub_action in sub_actions:
            for name, sub_parser in sub_action.choices.items():
                label = f"{prefix} {name}".strip()
                # Recurse first to detect leaves.
                nested = [
                    a for a in sub_parser._actions  # noqa: SLF001
                    if isinstance(a, argparse._SubParsersAction)  # noqa: SLF001
                    and a.choices
                ]
                if nested:
                    _walk(sub_parser, label)
                    continue
                args: list[str] = []
                for action in sub_parser._actions:  # noqa: SLF001
                    rendered = _describe_argparse_action(action)
                    if rendered is not None:
                        args.append(rendered)
                description = ""
                # Recover the parent's help text for this subcommand.
                # argparse stores per-choice help on
                # ``_SubParsersAction._choices_actions`` (a private list of
                # ``_ChoicesPseudoAction`` whose ``dest`` matches the name).
                choices_actions = getattr(
                    sub_action, "_choices_actions", []) or []
                for ca in choices_actions:
                    if ca.dest == name:
                        description = ca.help or ""
                        break
                rows.append({
                    "subcommand": label,
                    "args": args,
                    "description": description,
                })

    _walk(parser, "")
    rows.sort(key=lambda r: str(r["subcommand"]))
    return rows


def cmd_routes(db, args) -> int:
    """Emit every CLI subcommand with its args and description.

    Reads the parser from ``_build_parser`` so the listing cannot drift
    from the actual surface — adding a subparser anywhere updates this
    output automatically.
    """
    parser, _, _ = _build_parser()
    rows = _collect_cli_routes(parser)
    if getattr(args, "json", False):
        print(json.dumps(rows, indent=2))
        return 0
    for row in rows:
        raw_args = row["args"]
        args_list = raw_args if isinstance(raw_args, list) else []
        args_str = " ".join(str(a) for a in args_list) if args_list else ""
        if args_str:
            print(f"{row['subcommand']}  [{args_str}]")
        else:
            print(f"{row['subcommand']}")
        desc = row["description"]
        if desc:
            print(f"    {desc}")
    return 0


def main():
    parser, p_sp, p_triage_op = _build_parser()
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    if args.command == "search-plan" and not getattr(
            args, "search_plan_command", None):
        p_sp.print_help()
        sys.exit(1)
    if args.command == "triage" and not getattr(
            args, "triage_command", None):
        p_triage_op.print_help()
        sys.exit(1)

    # ``routes`` is the only subcommand that doesn't require a DB
    # connection — short-circuit before constructing PipelineDB so the
    # command works without a reachable database.
    if args.command == "routes":
        rc = cmd_routes(None, args)
        if isinstance(rc, int):
            sys.exit(rc)
        return

    db = PipelineDB(args.dsn)

    commands = {
        "list": cmd_list,
        "add": cmd_add,
        "query": cmd_query,
        "status": cmd_status,
        "disk-coverage": cmd_disk_coverage,
        "retry": cmd_retry,
        "cancel": cmd_cancel,
        "set": cmd_set,
        "set-intent": cmd_set_intent,
        "show": cmd_show,
        "quality": cmd_quality,
        "force-import": cmd_force_import,
        "manual-import": cmd_manual_import,
        "import-jobs": cmd_import_jobs,
        "import-preview": cmd_import_preview,
        "wrong-match-triage": cmd_wrong_match_triage,
        "wrong-match-delete": cmd_wrong_match_delete,
        "wrong-match-delete-group": cmd_wrong_match_delete_group,
        "repair-spectral": cmd_repair_spectral,
        "replace": cmd_replace,
        "beets-distance": cmd_beets_distance,
        "youtube-album": cmd_youtube_album,
        "youtube-rescue": cmd_youtube_rescue,
        "long-tail": cmd_long_tail,
    }
    search_plan_commands = {
        "show": cmd_search_plan_show,
        "regenerate": cmd_search_plan_regenerate,
        "advance": cmd_search_plan_advance,
        "history": cmd_search_plan_history,
        "dry-run": cmd_search_plan_dry_run,
        "saturation": cmd_search_plan_saturation,
    }
    triage_commands = {
        "show": cmd_triage_show,
        "list": cmd_triage_list,
    }
    try:
        if args.command == "search-plan":
            rc = search_plan_commands[args.search_plan_command](db, args)
        elif args.command == "triage":
            rc = triage_commands[args.triage_command](db, args)
        else:
            rc = commands[args.command](db, args)
    finally:
        db.close()
    if isinstance(rc, int):
        sys.exit(rc)


if __name__ == "__main__":
    main()
