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
import os
import subprocess
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, time
from decimal import Decimal

import psycopg2

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, REPO_ROOT)
sys.path.insert(0, os.path.join(REPO_ROOT, "lib"))
from pipeline_db import PipelineDB, DEFAULT_DSN

MB_API = "http://192.168.1.35:5200/ws/2"


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
    mbid = args.mbid
    source = args.source

    # Check if already exists
    existing = db.get_request_by_mb_release_id(mbid)
    if existing:
        print(f"  Already in DB: id={existing['id']} status={existing['status']}")
        return

    # Fetch from MB API
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

    # Populate tracks
    tracks = tracks_from_mb_release(release)
    if tracks:
        db.set_tracks(req_id, tracks)

    print(f"  Added: id={req_id} {artist_name} - {release.get('title')} ({len(tracks)} tracks)")


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
    from lib.transitions import apply_transition
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    apply_transition(db, args.id, "wanted", from_status=req["status"])
    print(f"  Reset to wanted: [{args.id}] {req['artist_name']} - {req['album_title']}")


def cmd_cancel(db, args):
    from lib.transitions import apply_transition
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    apply_transition(db, args.id, "manual", from_status=req["status"])
    print(f"  Marked for manual download: [{args.id}] {req['artist_name']} - {req['album_title']}")


VALID_STATUSES = ["wanted", "imported", "manual"]


def cmd_set(db, args):
    from lib.transitions import apply_transition
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    old_status = req["status"]
    if old_status == args.status:
        print(f"  [{args.id}] already has status '{args.status}'.")
        return
    apply_transition(db, args.id, args.status, from_status=old_status)
    print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: {old_status} → {args.status}")


def cmd_set_intent(db, args):
    """Set quality intent for a request."""
    from lib.quality import QualityIntent, intent_to_quality_override
    from lib.transitions import apply_transition
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    if req["status"] == "downloading":
        print(f"  Cannot set intent while album is downloading.")
        return
    intent = QualityIntent(args.intent)
    quality_override = intent_to_quality_override(intent)
    old_override = req.get("quality_override")

    if req["status"] == "imported":
        min_br = req.get("min_bitrate")
        apply_transition(db, args.id, "wanted", from_status="imported",
                         quality_override=quality_override,
                         min_bitrate=min_br)
        print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: "
              f"intent={intent.value}, re-queued for search")
    else:
        db._execute(
            "UPDATE album_requests SET quality_override = %s, updated_at = NOW() WHERE id = %s",
            (quality_override, args.id),
        )
        print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: "
              f"intent={intent.value} (override: {old_override} → {quality_override})")


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
    q_override = req.get("quality_override")
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
            print(f"    quality_override:  {q_override}")

    tracks = db.get_tracks(req['id'])
    if tracks:
        print(f"\n  Tracks ({len(tracks)}):")
        for t in tracks:
            dur = f"{t['length_seconds']:.0f}s" if t['length_seconds'] else "?"
            print(f"    {t['disc_number']}.{t['track_number']:02d} {t['title']} ({dur})")

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
    from quality import full_pipeline_decision, quality_gate_decision, AudioQualityMeasurement

    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return

    label = f"{req['artist_name']} - {req['album_title']}"
    min_br = req.get("min_bitrate")
    verified = bool(req.get("verified_lossless"))
    current_br = req.get("current_spectral_bitrate")
    q_override = req.get("quality_override")

    print(f"  {label}")
    print(f"  Status: {req['status']}")
    print()

    # --- Current quality gate ---
    if min_br is not None:
        from beets_db import BeetsDB
        mbid = req.get("mb_release_id")
        is_cbr = False
        if mbid:
            try:
                with BeetsDB() as beets:
                    info = beets.get_album_info(mbid)
                if info:
                    is_cbr = info.is_cbr
            except Exception:
                pass
        current = AudioQualityMeasurement(
            min_bitrate_kbps=min_br, is_cbr=is_cbr,
            verified_lossless=verified,
            spectral_bitrate_kbps=current_br)
        gate = quality_gate_decision(current)
        gate_label = {"accept": "DONE", "requeue_upgrade": "NEEDS UPGRADE",
                      "requeue_flac": "NEEDS FLAC"}[gate]
        print(f"  Quality gate:  {gate_label}")
        print(f"    min_bitrate={_fmt_br(min_br)}, verified_lossless={verified}, "
              f"is_cbr={is_cbr}")
        if current_br:
            print(f"    current_spectral_bitrate={current_br}kbps")
        if q_override:
            print(f"    searching: {q_override}")
    else:
        print(f"  Quality gate:  NO DATA (not yet imported)")

    # --- Simulate common scenarios ---
    effective_existing = min_br
    if current_br and min_br and current_br < min_br:
        effective_existing = current_br

    scenarios = [
        ("Genuine FLAC → V0", dict(
            is_flac=True, min_bitrate=245, is_cbr=False,
            spectral_grade="genuine", converted_count=12,
            post_conversion_min_bitrate=245)),
        ("MP3 V0 (240kbps)", dict(
            is_flac=False, min_bitrate=240, is_cbr=False)),
        ("MP3 CBR 320", dict(
            is_flac=False, min_bitrate=320, is_cbr=True)),
        ("Suspect FLAC (transcode)", dict(
            is_flac=True, min_bitrate=190, is_cbr=False,
            spectral_grade="suspect", converted_count=12,
            post_conversion_min_bitrate=190)),
    ]

    print(f"\n  What would happen if we downloaded:")
    for name, params in scenarios:
        result = full_pipeline_decision(
            existing_min_bitrate=effective_existing,
            existing_spectral_bitrate=current_br,
            override_min_bitrate=current_br if current_br and min_br and current_br < min_br else None,
            verified_lossless=verified,
            **params)

        imported = "IMPORT" if result["imported"] else "REJECT"
        parts = [imported]
        if result["denylisted"]:
            parts.append("denylist")
        if result["keep_searching"]:
            parts.append("keep searching")
        final = result["final_status"] or "?"
        decision_chain = " → ".join(
            f"{s}={result[s]}" for s in ["stage1_spectral", "stage2_import", "stage3_quality_gate"]
            if result[s] is not None)

        print(f"    {name}:")
        print(f"      → {', '.join(parts)} (final: {final})")
        if decision_chain:
            print(f"      chain: {decision_chain}")


IMPORT_ONE = os.path.join(os.path.dirname(__file__), "..", "harness", "import_one.py")

# Known slskd download dirs to resolve old relative failed_paths against
SLSKD_DOWNLOAD_DIRS = ["/mnt/virtio/music/slskd"]


def _resolve_failed_path(failed_path: str) -> "str | None":
    """Resolve a failed_path to an existing absolute directory.

    Old entries stored relative paths (e.g. 'failed_imports/Foo - Bar').
    New entries store absolute paths. Try the path as-is first, then
    resolve against known slskd download dirs.
    """
    if os.path.isdir(failed_path):
        return failed_path
    for base in SLSKD_DOWNLOAD_DIRS:
        candidate = os.path.join(base, failed_path)
        if os.path.isdir(candidate):
            return candidate
    return None


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

    from lib.import_service import run_import, log_and_update_import
    outcome = run_import(
        failed_path, mbid,
        request_id=request_id,
        import_one_path=IMPORT_ONE,
        force=True,
        override_min_bitrate=req["min_bitrate"],
    )
    log_and_update_import(db, request_id, outcome,
                          outcome_label="force_import",
                          staged_path=failed_path)
    if outcome.success:
        print(f"  [OK] Force-import successful (exit code 0)")
    else:
        print(f"  [WARN] {outcome.message}")


def cmd_manual_import(db, args):
    """Import a local folder as a pipeline request."""
    from lib.import_service import run_import, log_and_update_import

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

    print(f"  Manual import: {req['artist_name']} - {req['album_title']}")
    print(f"  Path: {path}")
    print(f"  MBID: {mbid}")

    outcome = run_import(
        path, mbid,
        request_id=request_id,
        import_one_path=IMPORT_ONE,
        override_min_bitrate=req["min_bitrate"],
    )
    log_and_update_import(db, request_id, outcome,
                          outcome_label="manual_import",
                          staged_path=path)
    if outcome.success:
        print(f"  [OK] {outcome.message}")
    else:
        print(f"  [FAIL] {outcome.message}")


def cmd_repair_spectral(db, args):
    """Find and repair albums stuck by stale current_spectral_bitrate.

    Identifies wanted albums where current_spectral_grade is genuine but
    current_spectral_bitrate still holds a stale transcode estimate,
    causing the quality gate to requeue indefinitely (issue #18).
    """
    from quality import AudioQualityMeasurement, quality_gate_decision

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
        min_br = req["min_bitrate"]
        stale_br = req["current_spectral_bitrate"]
        print(f"  [{rid:>4}] {label}")
        print(f"         min_bitrate={min_br}kbps, stale current_spectral={stale_br}kbps")

        # Check what quality gate would decide after clearing stale data
        measurement = AudioQualityMeasurement(
            min_bitrate_kbps=min_br or 0,
            is_cbr=False,  # if they're genuine, they're VBR V0
            verified_lossless=bool(req.get("verified_lossless")),
            spectral_bitrate_kbps=None,  # cleared
        )
        decision = quality_gate_decision(measurement)
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
        if decision == "accept" and min_br:
            db._execute("""
                UPDATE album_requests
                SET status = 'imported',
                    min_bitrate = %s,
                    updated_at = NOW()
                WHERE id = %s
            """, (min_br, rid))
            print(f"         → transitioned to imported")
        else:
            print(f"         → remains wanted (gate says {decision})")

        repaired += 1

    print(f"\nRepaired {repaired} album(s)." if not args.dry_run
          else f"\n[DRY RUN] Would repair {len(candidates)} album(s).")


def main():
    parser = argparse.ArgumentParser(description="Pipeline CLI — manage download pipeline DB")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    sub = parser.add_subparsers(dest="command")

    # list
    p_list = sub.add_parser("list", help="List album requests")
    p_list.add_argument("filter_status", nargs="?", help="Filter by status")

    # add
    p_add = sub.add_parser("add", help="Add a new request by MBID")
    p_add.add_argument("mbid", help="MusicBrainz release ID")
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

    # quality
    p_quality = sub.add_parser("quality", help="Show quality state and simulate decisions")
    p_quality.add_argument("id", type=int, help="Request ID")

    # set-intent
    p_intent = sub.add_parser("set-intent", help="Set quality intent for a request")
    p_intent.add_argument("id", type=int, help="Request ID")
    p_intent.add_argument("intent", choices=["best_effort", "flac_only", "flac_preferred", "upgrade"],
                          help="Quality intent")

    # force-import
    p_force = sub.add_parser("force-import", help="Force-import a rejected download by download_log ID")
    p_force.add_argument("download_log_id", type=int, help="Download log ID")

    # manual-import
    p_manual = sub.add_parser("manual-import", help="Import a local folder as a pipeline request")
    p_manual.add_argument("id", type=int, help="Pipeline request ID")
    p_manual.add_argument("path", help="Path to album folder")

    # repair-spectral
    p_repair = sub.add_parser("repair-spectral",
                              help="Fix albums stuck by stale current_spectral_bitrate (#18)")
    p_repair.add_argument("--dry-run", action="store_true",
                          help="Show what would be repaired without changing anything")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    db = PipelineDB(args.dsn, run_migrations=True)

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
        "repair-spectral": cmd_repair_spectral,
    }
    commands[args.command](db, args)
    db.close()


if __name__ == "__main__":
    main()
