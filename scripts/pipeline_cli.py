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
from lib.release_identity import detect_release_source, normalize_release_id
from lib.util import resolve_failed_path as _shared_resolve_failed_path

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
    """Fetch release metadata + tracks from MusicBrainz API."""
    url = f"{MB_API}/release/{mb_release_id}?inc=recordings+artist-credits&fmt=json"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "pipeline-cli/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (urllib.error.HTTPError, urllib.error.URLError) as e:
        print(f"  [ERROR] MB API: {e}", file=sys.stderr)
        return None


def fetch_mb_release_group_year(rg_mbid):
    """Return the release-group's first-release year as an int, or None.

    U4 enqueue-time companion to ``fetch_mb_release``. Returns ``None``
    on 404 / unparseable date / network blip — callers persist NULL on
    the new ``release_group_year`` column rather than failing the add.
    Mirrors ``web/mb.py::get_release_group_year`` but goes through
    ``urllib`` directly so the CLI stays decoupled from the Redis-backed
    web client.
    """
    url = f"{MB_API}/release-group/{rg_mbid}?fmt=json"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "pipeline-cli/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        print(f"  [WARN] MB release-group fetch: {e}", file=sys.stderr)
        return None
    except urllib.error.URLError as e:
        print(f"  [WARN] MB release-group fetch: {e}", file=sys.stderr)
        return None
    date = data.get("first-release-date", "")
    if not isinstance(date, str) or len(date) < 4:
        return None
    try:
        return int(date[:4])
    except ValueError:
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


def _generate_plan_after_add(db, req_id, *, artist_name, album_title, year,
                              tracks, source):
    """Run plan generation for a freshly-added request.

    Failures are non-fatal: a deterministic / transient failure is
    recorded as a `search_plans` row and the CLI prints a one-liner so
    the operator knows the request is wanted-but-not-searchable until
    repaired.
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
    )
    if result.outcome == RESULT_SUCCESS:
        print(f"  Plan: active id={result.plan_id}")
    elif result.outcome == RESULT_FAILED_DETERMINISTIC:
        print(f"  Plan: FAILED ({result.failure_class}); request not searchable")
    elif result.outcome == RESULT_FAILED_TRANSIENT:
        print(f"  Plan: TRANSIENT FAIL ({result.failure_class}); will retry")


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

    # U4: persist release-group year so the generator (U5) can emit a
    # year-anchored slot matching how users on Soulseek file reissues.
    # ``fetch_mb_release_group_year`` is 404/error tolerant; column
    # accepts NULL.
    rg_year = fetch_mb_release_group_year(rg_id) if rg_id else None

    req_id = db.add_request(
        mb_release_id=mbid,
        mb_release_group_id=rg_id,
        mb_artist_id=artist_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        release_group_year=rg_year,
        country=release.get("country"),
        source=source,
    )

    tracks = tracks_from_mb_release(release)
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {artist_name} - {release.get('title')} ({len(tracks)} tracks)")
    _generate_plan_after_add(
        db, req_id,
        artist_name=artist_name,
        album_title=release.get("title", "Unknown"),
        year=year,
        tracks=tracks,
        source=source,
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
    _generate_plan_after_add(
        db, req_id,
        artist_name=release["artist_name"],
        album_title=release["title"],
        year=release.get("year"),
        tracks=tracks,
        source=source,
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
    transitions.finalize_request(
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
    transitions.finalize_request(
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
    transitions.finalize_request(
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
        transitions.finalize_request(
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

    # Top-3 by (matched_tracks DESC, avg_ratio DESC) — same ordering as the
    # web UI route, so CLI and web surfaces show the same scoring. Shared
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
    print(f"  Country:      {req['country']}")
    print(f"  Format:       {req['format']}")
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
            print(f"    [{h['created_at']}] {h['outcome']} from {h['soulseek_username']} "
                  f"(dist={h['beets_distance']})")
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
            transitions.finalize_request(
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
        prepend_artist=bool(getattr(args, "prepend_artist", False)),
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
        RESULT_ADVANCED,
        RESULT_FAILED_TRANSIENT,
        RESULT_INVALID_TARGET,
        RESULT_NO_ACTIVE_PLAN,
        RESULT_REQUEST_NOT_FOUND,
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

    if result.outcome == RESULT_ADVANCED:
        return 0
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        return 2
    if result.outcome == RESULT_INVALID_TARGET:
        return 3
    if result.outcome == RESULT_NO_ACTIVE_PLAN:
        return 4
    if result.outcome == RESULT_FAILED_TRANSIENT:
        return 5
    return 1


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
        RESULT_HISTORY_PAGE_INPUT_INVALID,
        RESULT_HISTORY_PAGE_SUCCESS,
        RESULT_REQUEST_NOT_FOUND,
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

    if result.outcome == RESULT_HISTORY_PAGE_SUCCESS:
        return 0
    if result.outcome == RESULT_REQUEST_NOT_FOUND:
        return 2
    if result.outcome == RESULT_HISTORY_PAGE_INPUT_INVALID:
        return 3
    return 1


def main():
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
                             dest="prepend_artist",
                             help="Prepend artist name to album title in "
                             "generated queries")
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

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)
    if args.command == "search-plan" and not getattr(
            args, "search_plan_command", None):
        p_sp.print_help()
        sys.exit(1)

    db = PipelineDB(args.dsn)

    commands = {
        "list": cmd_list,
        "add": cmd_add,
        "query": cmd_query,
        "status": cmd_status,
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
    }
    search_plan_commands = {
        "show": cmd_search_plan_show,
        "regenerate": cmd_search_plan_regenerate,
        "advance": cmd_search_plan_advance,
        "history": cmd_search_plan_history,
    }
    try:
        if args.command == "search-plan":
            rc = search_plan_commands[args.search_plan_command](db, args)
        else:
            rc = commands[args.command](db, args)
    finally:
        db.close()
    if isinstance(rc, int):
        sys.exit(rc)


if __name__ == "__main__":
    main()
