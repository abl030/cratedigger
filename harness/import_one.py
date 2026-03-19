#!/usr/bin/env python3
"""One-shot beets import for a single album with a known MBID.

Designed for the pipeline DB auto-import path (source='request').
Pre-flight checks beets DB, converts FLAC→V0, imports via harness,
post-flight verifies exact MBID in beets DB.

Usage:
    python3 import_one.py <album_path> <mb_release_id> [--request-id N] [--dry-run]

Exit codes:
    0 = imported (or already in beets)
    1 = FLAC conversion failed
    2 = beets import failed (harness error, post-flight verification failed)
    3 = album path not found
    4 = MBID not found in beets candidates
"""

import argparse
import json
import os
import select
import signal
import sqlite3
import subprocess
import sys

BEETS_DB = "/mnt/virtio/Music/beets-library.db"
HARNESS = os.path.join(os.path.dirname(__file__), "..", "harness", "run_beets_harness.sh")
HARNESS_TIMEOUT = 300
IMPORT_TIMEOUT = 1800
MAX_DISTANCE = 0.5


# ---------------------------------------------------------------------------
# Pre-flight / post-flight — query beets DB directly
# ---------------------------------------------------------------------------

def preflight_check(mb_release_id):
    """Return True if this MBID is already in the beets library."""
    conn = sqlite3.connect(BEETS_DB)
    row = conn.execute(
        "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
    ).fetchone()
    conn.close()
    return row is not None


def postflight_verify(mb_release_id):
    """Verify the MBID was imported. Returns (album_id, track_count, album_path) or None."""
    conn = sqlite3.connect(BEETS_DB)
    row = conn.execute(
        "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
    ).fetchone()
    if not row:
        conn.close()
        return None

    album_id = row[0]
    track_count = conn.execute(
        "SELECT COUNT(*) FROM items WHERE album_id = ?", (album_id,)
    ).fetchone()[0]

    path_row = conn.execute(
        "SELECT path FROM items WHERE album_id = ? LIMIT 1", (album_id,)
    ).fetchone()
    album_path = None
    if path_row:
        # path is stored as bytes in beets DB
        raw = path_row[0]
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        album_path = os.path.dirname(raw)

    conn.close()
    return album_id, track_count, album_path


# ---------------------------------------------------------------------------
# FLAC → MP3 VBR V0 conversion
# ---------------------------------------------------------------------------

def convert_flac_to_v0(album_path, dry_run=False):
    """Convert all FLAC files to MP3 VBR V0. Returns (converted, failed)."""
    flac_files = sorted(f for f in os.listdir(album_path) if f.lower().endswith(".flac"))
    if not flac_files:
        return 0, 0

    converted = 0
    failed = 0
    for fname in flac_files:
        flac_path = os.path.join(album_path, fname)
        mp3_path = os.path.splitext(flac_path)[0] + ".mp3"

        if os.path.exists(mp3_path):
            continue

        if dry_run:
            print(f"  [DRY] {fname} → {os.path.basename(mp3_path)}")
            converted += 1
            continue

        result = subprocess.run([
            "ffmpeg", "-i", flac_path,
            "-codec:a", "libmp3lame", "-q:a", "0",
            "-map_metadata", "0", "-id3v2_version", "3",
            "-y", mp3_path,
        ], capture_output=True, text=True)

        if result.returncode != 0 or not os.path.exists(mp3_path) or os.path.getsize(mp3_path) == 0:
            print(f"  [FAIL] {fname}: {result.stderr[-200:]}", file=sys.stderr)
            if os.path.exists(mp3_path):
                os.remove(mp3_path)
            failed += 1
        else:
            os.remove(flac_path)
            converted += 1

    return converted, failed


# ---------------------------------------------------------------------------
# Beets harness controller (JSON protocol)
# ---------------------------------------------------------------------------

def run_import(path, mb_release_id):
    """Drive the beets harness to import one album. Returns exit code."""
    cmd = [HARNESS, "--noincremental", "--search-id", mb_release_id, path]
    print(f"  [HARNESS] {' '.join(cmd)}")

    env = {**os.environ, "HOME": "/home/abl030"}
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid, env=env,
    )

    applied = False
    timeout = HARNESS_TIMEOUT

    try:
        while True:
            ready, _, _ = select.select([proc.stdout.fileno()], [], [], timeout)
            if not ready:
                print(f"  [TIMEOUT] No output for {timeout}s", file=sys.stderr)
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait()
                return 2

            line = proc.stdout.readline()
            if not line:
                break

            line = line.strip()
            if not line:
                continue

            try:
                msg = json.loads(line)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type in ("session_start", "session_end", "error"):
                if msg_type == "error":
                    print(f"  [HARNESS ERROR] {msg.get('message', '')}", file=sys.stderr)
                continue

            elif msg_type == "should_resume":
                proc.stdin.write(json.dumps({"resume": False}) + "\n")
                proc.stdin.flush()

            elif msg_type == "resolve_duplicate":
                # Pre-flight verified MBID not in DB. If we get here, it's a
                # stale/partial entry. Remove it and keep the new import.
                proc.stdin.write(json.dumps({"action": "remove"}) + "\n")
                proc.stdin.flush()
                print(f"  [DUP] Removing stale entry, keeping new import")

            elif msg_type in ("choose_match", "choose_item"):
                candidates = msg.get("candidates", [])

                # Find candidate matching our target MBID
                matched_idx = None
                for i, c in enumerate(candidates):
                    if c.get("album_id", "") == mb_release_id:
                        matched_idx = i
                        break

                if matched_idx is None:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    avail = [c.get("album_id", "?") for c in candidates]
                    print(f"  [SKIP] MBID {mb_release_id} not in {len(candidates)} candidates: {avail}",
                          file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return 4

                cand = candidates[matched_idx]
                dist = cand.get("distance", 1.0)

                if dist > MAX_DISTANCE:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [REJECT] distance={dist:.4f} > {MAX_DISTANCE}", file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return 2

                proc.stdin.write(json.dumps({"action": "apply", "candidate_index": matched_idx}) + "\n")
                proc.stdin.flush()
                applied = True
                timeout = IMPORT_TIMEOUT
                print(f"  [APPLY] {cand.get('artist')} - {cand.get('album')} (dist={dist:.4f})")

    except BrokenPipeError:
        print("  [WARN] Harness pipe broken", file=sys.stderr)

    if proc.poll() is None:
        proc.wait()

    stderr_out = proc.stderr.read() if proc.stderr else ""
    if stderr_out.strip():
        # Only log non-trivial stderr
        for line in stderr_out.strip().split("\n"):
            if "Disabled fetchart" not in line:
                print(f"  [BEETS] {line}", file=sys.stderr)

    return 0 if applied else 2


# ---------------------------------------------------------------------------
# Pipeline DB updates
# ---------------------------------------------------------------------------

def update_pipeline_db(request_id, status, imported_path=None, distance=None, scenario=None):
    """Update pipeline DB status. Best-effort — failures logged but don't block."""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
        from pipeline_db import PipelineDB
        db = PipelineDB()
        extra = {}
        if imported_path:
            extra["imported_path"] = imported_path
        if distance is not None:
            extra["beets_distance"] = distance
        if scenario:
            extra["beets_scenario"] = scenario
        db.update_status(request_id, status, **extra)
        db.close()
    except Exception as e:
        print(f"  [WARN] Pipeline DB update failed: {e}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="One-shot beets import for a single album")
    parser.add_argument("path", help="Path to staged album directory")
    parser.add_argument("mb_release_id", help="MusicBrainz release ID")
    parser.add_argument("--request-id", type=int, help="Pipeline DB request ID for status updates")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mbid = args.mb_release_id
    request_id = args.request_id

    # --- Pre-flight: already imported? ---
    if preflight_check(mbid):
        print(f"[PRE-FLIGHT] Already in beets: {mbid}")
        if request_id:
            result = postflight_verify(mbid)
            if result:
                _, track_count, album_path = result
                update_pipeline_db(request_id, "imported", imported_path=album_path)
        sys.exit(0)

    # --- Path check ---
    if not os.path.isdir(args.path):
        print(f"[ERROR] Path not found: {args.path}", file=sys.stderr)
        sys.exit(3)

    # --- Convert FLAC → V0 ---
    if request_id:
        update_pipeline_db(request_id, "converting")

    print(f"[CONVERT] {args.path}")
    converted, failed = convert_flac_to_v0(args.path, dry_run=args.dry_run)
    print(f"  Converted {converted}, failed {failed}")
    if failed > 0:
        print("[ERROR] Conversion failures — aborting", file=sys.stderr)
        if request_id:
            update_pipeline_db(request_id, "review_needed")
        sys.exit(1)

    if args.dry_run:
        print("[DRY] Would import via harness")
        sys.exit(0)

    # --- Import ---
    if request_id:
        update_pipeline_db(request_id, "importing")

    print(f"[IMPORT] {args.path} → beets (mbid={mbid})")
    rc = run_import(args.path, mbid)

    if rc != 0:
        print(f"[ERROR] Import failed (rc={rc})", file=sys.stderr)
        if request_id:
            update_pipeline_db(request_id, "review_needed")
        sys.exit(rc)

    # --- Post-flight verification ---
    result = postflight_verify(mbid)
    if not result:
        print(f"[ERROR] Post-flight: MBID {mbid} NOT in beets DB after import", file=sys.stderr)
        if request_id:
            update_pipeline_db(request_id, "review_needed")
        sys.exit(2)

    album_id, track_count, album_path = result
    print(f"[POST-FLIGHT OK] mbid={mbid}, beets_id={album_id}, tracks={track_count}, path={album_path}")

    # --- Cleanup staged dir ---
    if os.path.isdir(args.path):
        for root, dirs, files in os.walk(args.path, topdown=False):
            for f in files:
                os.remove(os.path.join(root, f))
            for d in dirs:
                try:
                    os.rmdir(os.path.join(root, d))
                except OSError:
                    pass
        try:
            os.rmdir(args.path)
        except OSError:
            pass
        # Try to remove parent artist dir if empty
        parent = os.path.dirname(args.path)
        try:
            os.rmdir(parent)
        except OSError:
            pass

    # --- Pipeline DB: imported ---
    if request_id:
        update_pipeline_db(request_id, "imported", imported_path=album_path)

    print("[OK] Import complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
