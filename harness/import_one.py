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
    5 = quality downgrade (new files worse than existing)
    6 = transcode detected — may or may not have imported:
        - If upgrade over existing: imported, but denylist user + keep searching
        - If not an upgrade: not imported, denylist user + keep searching
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
TRANSCODE_MIN_BITRATE_KBPS = 210  # V0 floor — converted FLAC below this is a transcode


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
# Quality checking
# ---------------------------------------------------------------------------

AUDIO_EXTENSIONS = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wma"}


def _get_beets_min_bitrate(mb_release_id):
    """Get min track bitrate (kbps) for an MBID already in beets. Returns None if not found."""
    try:
        conn = sqlite3.connect(BEETS_DB)
        row = conn.execute(
            "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
        ).fetchone()
        if not row:
            conn.close()
            return None
        br = conn.execute(
            "SELECT MIN(bitrate) FROM items WHERE album_id = ?", (row[0],)
        ).fetchone()
        conn.close()
        if br and br[0] and br[0] > 0:
            return int(br[0] / 1000)
        return None
    except Exception:
        return None


def _get_folder_min_bitrate(folder_path):
    """Get min bitrate (kbps) of audio files in a folder via ffprobe.

    Uses audio stream bitrate (excludes cover art overhead). Falls back
    to format bitrate for VBR MP3s where stream bitrate is N/A.
    """
    min_br = None
    for fname in os.listdir(folder_path):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in AUDIO_EXTENSIONS:
            continue
        fpath = os.path.join(folder_path, fname)
        try:
            # Try audio stream bitrate first (accurate for CBR, excludes cover art)
            result = subprocess.run(
                ["ffprobe", "-v", "error",
                 "-select_streams", "a:0",
                 "-show_entries", "stream=bit_rate",
                 "-of", "csv=p=0", fpath],
                capture_output=True, text=True, timeout=30,
            )
            br_str = result.stdout.strip()
            # VBR MP3s return N/A for stream bitrate — fall back to format
            if not br_str or not br_str.isdigit():
                result = subprocess.run(
                    ["ffprobe", "-v", "error",
                     "-show_entries", "format=bit_rate",
                     "-of", "csv=p=0", fpath],
                    capture_output=True, text=True, timeout=30,
                )
                br_str = result.stdout.strip()
            if br_str and br_str.isdigit():
                br_kbps = int(br_str) // 1000
                if br_kbps > 0 and (min_br is None or br_kbps < min_br):
                    min_br = br_kbps
        except Exception:
            continue
    return min_br


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
                dup_mbids = msg.get("duplicate_mbids", [])
                if mb_release_id in dup_mbids:
                    # Same MBID already in DB — stale/partial entry, replace it.
                    proc.stdin.write(json.dumps({"action": "remove"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [DUP] Same MBID in library, removing stale entry")
                else:
                    # Different edition of same album — keep both, let %aunique{} disambiguate.
                    proc.stdin.write(json.dumps({"action": "keep"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [DUP] Different edition (existing: {dup_mbids}), keeping both")

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
        dsn = os.environ.get("PIPELINE_DB_DSN", "postgresql://soularr@localhost/soularr")
        db = PipelineDB(dsn)
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
    parser.add_argument("--override-min-bitrate", type=int, default=None,
                        help="Override existing min bitrate for downgrade check (kbps)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mbid = args.mb_release_id
    request_id = args.request_id

    # --- Pre-flight: already imported? ---
    already_in_beets = preflight_check(mbid)
    if already_in_beets:
        print(f"[PRE-FLIGHT] Already in beets: {mbid} — checking if new files are better")

    # --- Path check ---
    if not os.path.isdir(args.path):
        if already_in_beets:
            # No new files to compare — just confirm existing import
            print(f"[PRE-FLIGHT] No new files, keeping existing import")
            if request_id:
                result = postflight_verify(mbid)
                if result:
                    _, track_count, album_path = result
                    update_pipeline_db(request_id, "imported", imported_path=album_path)
            sys.exit(0)
        print(f"[ERROR] Path not found: {args.path}", file=sys.stderr)
        sys.exit(3)

    # --- Spectral analysis (pre-conversion) ---
    try:
        lib_dir = os.path.join(os.path.dirname(__file__), "..", "lib")
        if lib_dir not in sys.path:
            sys.path.insert(0, lib_dir)
        from spectral_check import analyze_album as spectral_analyze
        spectral_result = spectral_analyze(args.path, trim_seconds=30)
        print(f"  spectral_grade={spectral_result.grade}")
        if spectral_result.estimated_bitrate_kbps is not None:
            print(f"  spectral_bitrate={spectral_result.estimated_bitrate_kbps}")
        if spectral_result.grade in ("suspect", "likely_transcode"):
            cliff_tracks = [t for t in spectral_result.tracks if t.cliff_detected]
            if cliff_tracks:
                print(f"  spectral_cliff={cliff_tracks[0].cliff_freq_hz}Hz")
        # Spectral check on existing beets files (if album already in beets)
        if already_in_beets:
            existing_path = None
            try:
                conn = sqlite3.connect(BEETS_DB)
                row = conn.execute(
                    "SELECT (SELECT path FROM items WHERE album_id = a.id LIMIT 1) "
                    "FROM albums a WHERE a.mb_albumid = ?", (mbid,)
                ).fetchone()
                conn.close()
                if row and row[0]:
                    p = row[0].decode() if isinstance(row[0], bytes) else row[0]
                    existing_path = os.path.dirname(p)
            except Exception:
                pass
            if existing_path and os.path.isdir(existing_path):
                existing_spectral = spectral_analyze(existing_path, trim_seconds=30)
                if existing_spectral.estimated_bitrate_kbps is not None:
                    print(f"  existing_spectral_bitrate={existing_spectral.estimated_bitrate_kbps}")
                print(f"  existing_spectral_grade={existing_spectral.grade}")
    except Exception as e:
        print(f"  [SPECTRAL] error: {e}", file=sys.stderr)

    # --- Convert FLAC → V0 ---
    print(f"[CONVERT] {args.path}")
    converted, failed = convert_flac_to_v0(args.path, dry_run=args.dry_run)
    print(f"  Converted {converted}, failed {failed}")
    if failed > 0:
        print("[ERROR] Conversion failures — aborting", file=sys.stderr)
        sys.exit(1)

    # --- Transcode detection ---
    # If we converted FLACs, check the resulting MP3 bitrate.
    # Genuine lossless→V0 produces ~220-260kbps. Sub-threshold means
    # the FLAC was a transcode (MP3 wrapped in FLAC container).
    is_transcode = False
    if converted > 0:
        post_conv_br = _get_folder_min_bitrate(args.path)
        if post_conv_br is not None and post_conv_br < TRANSCODE_MIN_BITRATE_KBPS:
            print(f"[TRANSCODE] converted FLAC min bitrate {post_conv_br}kbps "
                  f"< {TRANSCODE_MIN_BITRATE_KBPS}kbps — source was not lossless")
            is_transcode = True
        if post_conv_br is not None:
            print(f"  min_bitrate={post_conv_br}")

    if args.dry_run:
        print("[DRY] Would import via harness")
        sys.exit(0)

    # --- Quality comparison ---
    # If this MBID is already in beets, check that new files are better.
    # For transcodes: still import if it's an upgrade, but exit 6 so
    # soularr denylists the user and keeps searching for real lossless.
    new_min_br = _get_folder_min_bitrate(args.path)
    existing_min_br = _get_beets_min_bitrate(mbid)
    # Pipeline DB may override existing bitrate (e.g. when existing files are
    # upsampled garbage — beets says 320 but spectral says 128)
    if args.override_min_bitrate is not None:
        effective_existing = args.override_min_bitrate
        if existing_min_br is not None and effective_existing != existing_min_br:
            print(f"  [OVERRIDE] pipeline says {effective_existing}kbps, beets says {existing_min_br}kbps")
    else:
        effective_existing = existing_min_br
    # Output both for soularr to track upgrade delta
    if effective_existing is not None:
        print(f"  prev_min_bitrate={effective_existing}")
    if new_min_br is not None:
        print(f"  new_min_bitrate={new_min_br}")
    # Genuine FLAC→V0 always wins over existing — V0 bitrate is numerically
    # lower than CBR 320 but objectively better quality (verified lossless source).
    will_be_verified_lossless = (converted > 0 and not is_transcode)
    if will_be_verified_lossless and effective_existing is not None:
        print(f"  [QUALITY CHECK] genuine FLAC→V0 at {new_min_br}kbps — "
              f"always upgrade over existing {effective_existing}kbps")
    elif effective_existing is not None and new_min_br is not None:
        if new_min_br <= effective_existing:
            print(f"[QUALITY DOWNGRADE] new {new_min_br}kbps <= existing {effective_existing}kbps — skipping import",
                  file=sys.stderr)
            if is_transcode:
                sys.exit(6)  # Transcode + not an upgrade
            sys.exit(5)
        print(f"  [QUALITY CHECK] new {new_min_br}kbps > existing {effective_existing}kbps — upgrading")
    elif existing_min_br is None and is_transcode:
        # First import — no existing quality to compare against.
        # Import the transcode (something is better than nothing).
        print(f"  [QUALITY CHECK] no existing album in beets — importing transcode")

    # --- Import ---
    print(f"[IMPORT] {args.path} → beets (mbid={mbid})")
    rc = run_import(args.path, mbid)

    if rc != 0:
        print(f"[ERROR] Import failed (rc={rc})", file=sys.stderr)
        sys.exit(rc)

    # --- Post-flight verification ---
    result = postflight_verify(mbid)
    if not result:
        print(f"[ERROR] Post-flight: MBID {mbid} NOT in beets DB after import", file=sys.stderr)
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

    if is_transcode:
        print("[OK] Transcode imported (upgrade) — denylist user, keep searching")
        sys.exit(6)
    print("[OK] Import complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
