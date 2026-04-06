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
import shutil
import subprocess
import sys
from dataclasses import dataclass
from typing import NoReturn

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LIB_DIR = os.path.join(ROOT_DIR, "lib")


def _bootstrap_import_paths() -> None:
    """Ensure standalone harness runs can import both lib.* and top-level modules."""
    for path in (ROOT_DIR, LIB_DIR):
        if path not in sys.path:
            sys.path.insert(0, path)


_bootstrap_import_paths()

from lib.beets_db import BeetsDB
from lib.quality import (AUDIO_EXTENSIONS_DOTTED as AUDIO_EXTENSIONS,
                         AudioQualityMeasurement, ImportResult,
                         PostflightInfo, TRANSCODE_MIN_BITRATE_KBPS,
                         import_quality_decision, transcode_detection)
HARNESS = os.path.join(os.path.dirname(__file__), "..", "harness", "run_beets_harness.sh")
BEET_BIN = (shutil.which("beet")
            or "/etc/profiles/per-user/abl030/bin/beet")
HARNESS_TIMEOUT = 300
IMPORT_TIMEOUT = 1800
MAX_DISTANCE = 0.5
_current_result: ImportResult | None = None


# ---------------------------------------------------------------------------
# Pure stage decision functions — extracted from main() for testability
# ---------------------------------------------------------------------------


@dataclass
class StageResult:
    """Result of a pipeline stage decision point."""
    decision: str = "continue"
    exit_code: int = 0
    error: str | None = None
    terminal: bool = False

    @property
    def is_terminal(self) -> bool:
        return self.terminal


def preflight_decision(already_in_beets: bool, path_exists: bool) -> StageResult:
    """Decide whether to proceed based on pre-flight checks (pure)."""
    if not path_exists:
        if already_in_beets:
            return StageResult(decision="preflight_existing", exit_code=0, terminal=True)
        return StageResult(decision="path_missing", exit_code=3,
                           error="Path not found", terminal=True)
    return StageResult(decision="continue")


def conversion_decision(converted: int, failed: int) -> StageResult:
    """Decide whether to proceed after FLAC conversion (pure)."""
    if failed > 0:
        return StageResult(decision="conversion_failed", exit_code=1,
                           error=f"{failed} FLAC files failed to convert",
                           terminal=True)
    return StageResult(decision="continue")


def quality_decision_stage(
    new: AudioQualityMeasurement,
    existing: AudioQualityMeasurement | None,
    is_transcode: bool,
) -> StageResult:
    """Run quality comparison and map to exit codes (pure wrapper).

    Delegates to import_quality_decision() and maps terminal decisions
    to exit codes: downgrade→5, transcode_downgrade→6.
    """
    decision = import_quality_decision(new, existing, is_transcode)

    if decision == "downgrade":
        return StageResult(decision="downgrade", exit_code=5, terminal=True)
    elif decision == "transcode_downgrade":
        return StageResult(decision="transcode_downgrade", exit_code=6, terminal=True)
    # import, transcode_upgrade, transcode_first all proceed to import
    return StageResult(decision=decision, exit_code=0)


def conversion_target(target_format: str | None,
                      will_be_verified_lossless: bool,
                      opus_conversion_enabled: bool) -> str:
    """What should lossless files become on disk? (pure)

    Returns: "flac" (keep as-is), "opus" (Opus 128), "v0" (MP3 V0).
    Easy to extend: add new formats as early returns.
    """
    if target_format == "flac":
        return "flac"
    if not will_be_verified_lossless:
        return "v0"
    if opus_conversion_enabled:
        return "opus"
    return "v0"


def opus_cleanup_decision(opus_skipped: bool, opus_conversion_enabled: bool,
                          converted: int) -> bool:
    """Should we clean up kept FLAC files after opus was skipped? (pure)

    When --opus-conversion was passed, convert_lossless_to_v0() kept the
    source files. If opus conversion was then skipped (transcode detected),
    we must remove the FLAC files so beets only sees V0 MP3s.
    """
    return opus_skipped and opus_conversion_enabled and converted > 0


def final_exit_decision(is_transcode: bool) -> int:
    """Determine the final exit code after a successful import."""
    return 6 if is_transcode else 0


# ---------------------------------------------------------------------------
# Quality checking
# ---------------------------------------------------------------------------


def _get_folder_min_bitrate(folder_path, ext_filter: set[str] | None = None):
    """Get min bitrate (kbps) of audio files in a folder via ffprobe.

    Uses audio stream bitrate (excludes cover art overhead). Falls back
    to format bitrate for VBR MP3s where stream bitrate is N/A.

    ext_filter: if provided, only measure files with these extensions
    (e.g. {".mp3"} to measure only V0 files when FLAC coexists).
    """
    min_br = None
    for fname in os.listdir(folder_path):
        ext = os.path.splitext(fname)[1].lower()
        if ext not in AUDIO_EXTENSIONS:
            continue
        if ext_filter is not None and ext not in ext_filter:
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
            br_str = result.stdout.strip().rstrip(",")
            # VBR MP3s return N/A for stream bitrate — fall back to format
            if not br_str or not br_str.isdigit():
                result = subprocess.run(
                    ["ffprobe", "-v", "error",
                     "-show_entries", "format=bit_rate",
                     "-of", "csv=p=0", fpath],
                    capture_output=True, text=True, timeout=30,
                )
                br_str = result.stdout.strip().rstrip(",")
            if br_str and br_str.isdigit():
                br_kbps = int(br_str) // 1000
                if br_kbps > 0 and (min_br is None or br_kbps < min_br):
                    min_br = br_kbps
        except Exception:
            continue
    return min_br


# ---------------------------------------------------------------------------
# Lossless → MP3 VBR V0 conversion
# ---------------------------------------------------------------------------

# Extensions that are always lossless
_ALWAYS_LOSSLESS_EXTS = {".flac", ".wav"}


def _is_m4a_alac(fpath: str) -> bool:
    """Check if an .m4a file contains ALAC (lossless) via ffprobe."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "a:0",
             "-show_entries", "stream=codec_name", "-of", "csv=p=0", fpath],
            capture_output=True, text=True, timeout=10)
        return result.stdout.strip().lower() == "alac"
    except Exception:
        return False


def _is_lossless_file(fname: str, folder: str = "") -> bool:
    """Check if a file is lossless. For .m4a, probes the codec with ffprobe."""
    ext = os.path.splitext(fname)[1].lower()
    if ext in _ALWAYS_LOSSLESS_EXTS:
        return True
    if ext == ".m4a":
        fpath = os.path.join(folder, fname) if folder else fname
        return _is_m4a_alac(fpath)
    return False


def convert_lossless_to_v0(album_path, dry_run=False, keep_source=False):
    """Convert all lossless files (FLAC, ALAC/m4a, WAV) to MP3 VBR V0.

    Returns (converted, failed, original_filetype) where original_filetype
    is the extension of the source files (e.g. "flac", "m4a", "wav").

    When keep_source=True, the original lossless files are preserved
    (used by Opus path which needs them for a second conversion).
    """
    lossless_files = sorted(f for f in os.listdir(album_path) if _is_lossless_file(f, album_path))
    if not lossless_files:
        return 0, 0, None

    # Track the original filetype from the first file
    original_ext = os.path.splitext(lossless_files[0])[1].lstrip(".").lower()

    converted = 0
    failed = 0
    for fname in lossless_files:
        src_path = os.path.join(album_path, fname)
        mp3_path = os.path.splitext(src_path)[0] + ".mp3"

        if os.path.exists(mp3_path):
            continue

        if dry_run:
            print(f"  [DRY] {fname} → {os.path.basename(mp3_path)}", file=sys.stderr)
            converted += 1
            continue

        try:
            result = subprocess.run([
                "ffmpeg", "-i", src_path,
                "-codec:a", "libmp3lame", "-q:a", "0",
                "-map_metadata", "0", "-id3v2_version", "3",
                "-y", mp3_path,
            ], capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            print(f"  [FAIL] {fname}: ffmpeg timed out after 300s", file=sys.stderr)
            if os.path.exists(mp3_path):
                os.remove(mp3_path)
            failed += 1
            continue

        if result.returncode != 0 or not os.path.exists(mp3_path) or os.path.getsize(mp3_path) == 0:
            print(f"  [FAIL] {fname}: {result.stderr[-200:]}", file=sys.stderr)
            if os.path.exists(mp3_path):
                os.remove(mp3_path)
            failed += 1
        else:
            if not keep_source:
                os.remove(src_path)
            converted += 1

    return converted, failed, original_ext


def convert_lossless_to_opus(album_path, dry_run=False):
    """Convert all lossless files to Opus 128kbps.

    Returns (converted, failed). Does NOT delete source files —
    the caller manages the lifecycle of lossless originals.

    Note: embedded cover art from FLAC does not transfer to Opus (Ogg
    containers don't support attached pictures via ffmpeg). Beets'
    fetchart plugin re-fetches art from Cover Art Archive during import.
    """
    lossless_files = sorted(f for f in os.listdir(album_path) if _is_lossless_file(f, album_path))
    if not lossless_files:
        return 0, 0

    converted = 0
    failed = 0
    for fname in lossless_files:
        src_path = os.path.join(album_path, fname)
        opus_path = os.path.splitext(src_path)[0] + ".opus"

        if os.path.exists(opus_path):
            continue

        if dry_run:
            print(f"  [DRY] {fname} → {os.path.basename(opus_path)}", file=sys.stderr)
            converted += 1
            continue

        try:
            result = subprocess.run([
                "ffmpeg", "-i", src_path,
                "-c:a", "libopus", "-b:a", "128k",
                "-map_metadata", "0",
                "-y", opus_path,
            ], capture_output=True, text=True, timeout=300)
        except subprocess.TimeoutExpired:
            print(f"  [FAIL] {fname}: ffmpeg timed out after 300s", file=sys.stderr)
            if os.path.exists(opus_path):
                os.remove(opus_path)
            failed += 1
            continue

        if result.returncode != 0 or not os.path.exists(opus_path) or os.path.getsize(opus_path) == 0:
            print(f"  [FAIL] {fname}: {result.stderr[-200:]}", file=sys.stderr)
            if os.path.exists(opus_path):
                os.remove(opus_path)
            failed += 1
        else:
            converted += 1

    return converted, failed


# ---------------------------------------------------------------------------
# Beets harness controller (JSON protocol)
# ---------------------------------------------------------------------------

def run_import(path, mb_release_id):
    """Drive the beets harness to import one album.

    Returns (exit_code, beets_lines, kept_duplicate) where kept_duplicate
    is True if we told beets to keep a different edition during duplicate
    resolution (triggers post-import `beet move` for %aunique disambiguation).
    """
    cmd = [HARNESS, "--noincremental", "--search-id", mb_release_id, path]
    print(f"  [HARNESS] {' '.join(cmd)}", file=sys.stderr)

    env = {**os.environ, "HOME": "/home/abl030"}
    proc = subprocess.Popen(
        cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True, preexec_fn=os.setsid, env=env,
    )
    assert proc.stdin is not None
    assert proc.stdout is not None
    assert proc.stderr is not None

    applied = False
    kept_duplicate = False
    timeout = HARNESS_TIMEOUT

    try:
        while True:
            ready, _, _ = select.select([proc.stdout.fileno()], [], [], timeout)
            if not ready:
                print(f"  [TIMEOUT] No output for {timeout}s", file=sys.stderr)
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                proc.wait()
                return 2, [], False

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
                    print(f"  [DUP] Same MBID in library, removing stale entry", file=sys.stderr)
                else:
                    # Different edition of same album — keep both.
                    # NOTE: beets %aunique{} does NOT fully disambiguate at
                    # import time (the new album gets "" if its disambiguator
                    # field is empty). We run `beet move` post-import to fix.
                    proc.stdin.write(json.dumps({"action": "keep"}) + "\n")
                    proc.stdin.flush()
                    kept_duplicate = True
                    print(f"  [DUP] Different edition (existing: {dup_mbids}), keeping both", file=sys.stderr)

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
                    return 4, [], False

                cand = candidates[matched_idx]
                dist = cand.get("distance", 1.0)

                if dist > MAX_DISTANCE:
                    proc.stdin.write(json.dumps({"action": "skip"}) + "\n")
                    proc.stdin.flush()
                    print(f"  [REJECT] distance={dist:.4f} > {MAX_DISTANCE}", file=sys.stderr)
                    if proc.poll() is None:
                        proc.wait()
                    return 2, [], False

                proc.stdin.write(json.dumps({"action": "apply", "candidate_index": matched_idx}) + "\n")
                proc.stdin.flush()
                applied = True
                timeout = IMPORT_TIMEOUT
                print(f"  [APPLY] {cand.get('artist')} - {cand.get('album')} (dist={dist:.4f})", file=sys.stderr)

    except BrokenPipeError:
        print("  [WARN] Harness pipe broken", file=sys.stderr)

    proc_rc = proc.wait() if proc.poll() is None else proc.poll()

    stderr_out = proc.stderr.read() if proc.stderr else ""
    beets_lines: list[str] = []
    if stderr_out.strip():
        for line in stderr_out.strip().split("\n"):
            if "Disabled fetchart" not in line:
                print(f"  [BEETS] {line}", file=sys.stderr)
                beets_lines.append(line.strip())

    if proc_rc not in (None, 0):
        return 2, beets_lines, kept_duplicate

    return (0 if applied else 2), beets_lines, kept_duplicate


# ---------------------------------------------------------------------------
# Pipeline DB updates
# ---------------------------------------------------------------------------

def update_pipeline_db(request_id, status, imported_path=None, distance=None, scenario=None):
    """Update pipeline DB status. Best-effort — failures logged but don't block."""
    try:
        from lib.pipeline_db import PipelineDB
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

def _log(msg):
    """Human-readable log to stderr (visible in journalctl)."""
    print(msg, file=sys.stderr, flush=True)


def _emit_and_exit(r) -> NoReturn:
    """Emit ImportResult JSON on stdout and exit."""
    print(r.to_sentinel_line(), flush=True)
    sys.exit(r.exit_code)


def main():
    parser = argparse.ArgumentParser(description="One-shot beets import for a single album")
    parser.add_argument("path", help="Path to staged album directory")
    parser.add_argument("mb_release_id", help="MusicBrainz release ID")
    parser.add_argument("--request-id", type=int, help="Pipeline DB request ID for status updates")
    parser.add_argument("--override-min-bitrate", type=int, default=None,
                        help="Override existing min bitrate for downgrade check (kbps)")
    parser.add_argument("--force", action="store_true",
                        help="Skip distance check (for force-importing rejected albums)")
    parser.add_argument("--opus-conversion", action="store_true",
                        help="Convert verified lossless to Opus 128 instead of keeping V0")
    parser.add_argument("--target-format", default=None,
                        help="Desired format on disk (e.g. 'flac' to skip conversion)")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    mbid = args.mb_release_id
    request_id = args.request_id

    # --force: raise distance threshold so high-distance candidates are accepted
    global MAX_DISTANCE
    if args.force:
        MAX_DISTANCE = 999
        _log("[FORCE] Distance check disabled (MAX_DISTANCE=999)")

    # Accumulate structured result (module-level so crash handler can preserve data)
    global _current_result  # noqa: PLW0603
    r = ImportResult()
    _current_result = r

    # --- Pre-flight: already imported? ---
    beets = BeetsDB()
    import atexit
    atexit.register(beets.close)
    already_in_beets = beets.album_exists(mbid)
    r.already_in_beets = already_in_beets
    if already_in_beets:
        _log(f"[PRE-FLIGHT] Already in beets: {mbid} — checking if new files are better")

    # --- Path check (pure decision) ---
    pf = preflight_decision(already_in_beets, os.path.isdir(args.path))
    if pf.is_terminal:
        r.decision = pf.decision
        r.exit_code = pf.exit_code
        r.error = pf.error
        if pf.decision == "preflight_existing":
            _log(f"[PRE-FLIGHT] No new files, keeping existing import")
            if request_id:
                info = beets.get_album_info(mbid)
                if info:
                    r.postflight = PostflightInfo(
                        beets_id=info.album_id,
                        track_count=info.track_count,
                        imported_path=info.album_path)
                    update_pipeline_db(request_id, "imported",
                                       imported_path=info.album_path)
        else:
            _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    # --- Spectral analysis (pre-conversion) ---
    spectral_grade: str | None = None
    spectral_bitrate: int | None = None
    existing_spectral_grade: str | None = None
    existing_spectral_bitrate: int | None = None
    try:
        from lib.spectral_check import analyze_album as spectral_analyze
        spectral_result = spectral_analyze(args.path, trim_seconds=30)
        spectral_grade = spectral_result.grade
        spectral_bitrate = spectral_result.estimated_bitrate_kbps
        r.spectral.suspect_pct = spectral_result.suspect_pct
        r.spectral.per_track = [
            {"grade": t.grade, "hf_deficit_db": round(t.hf_deficit_db, 1),
             "cliff_detected": t.cliff_detected,
             "cliff_freq_hz": t.cliff_freq_hz,
             "estimated_bitrate_kbps": t.estimated_bitrate_kbps}
            for t in spectral_result.tracks
        ]
        _log(f"  spectral_grade={spectral_grade}")
        if spectral_bitrate is not None:
            _log(f"  spectral_bitrate={spectral_bitrate}")
        if spectral_grade in ("suspect", "likely_transcode"):
            cliff_tracks = [t for t in spectral_result.tracks if t.cliff_detected]
            if cliff_tracks:
                r.spectral.cliff_freq_hz = cliff_tracks[0].cliff_freq_hz
                _log(f"  spectral_cliff={cliff_tracks[0].cliff_freq_hz}Hz")
        # Spectral check on existing beets files
        if already_in_beets:
            existing_path = beets.get_album_path(mbid)
            if existing_path and os.path.isdir(existing_path):
                existing_spectral = spectral_analyze(existing_path, trim_seconds=30)
                existing_spectral_grade = existing_spectral.grade
                existing_spectral_bitrate = existing_spectral.estimated_bitrate_kbps
                r.spectral.existing_suspect_pct = existing_spectral.suspect_pct
                _log(f"  existing_spectral_grade={existing_spectral_grade}")
                if existing_spectral_bitrate is not None:
                    _log(f"  existing_spectral_bitrate={existing_spectral_bitrate}")
    except Exception as e:
        _log(f"  [SPECTRAL] error: {e}")

    # --- Convert lossless → V0 (unless target_format=flac) ---
    converted = 0
    failed = 0
    original_ext = None
    v0_ext_filter = None
    post_conv_br = None
    is_transcode = False

    if args.target_format != "flac":
        _log(f"[CONVERT] {args.path}")
        converted, failed, original_ext = convert_lossless_to_v0(
            args.path, dry_run=args.dry_run,
            keep_source=args.opus_conversion)
        r.conversion.converted = converted
        r.conversion.failed = failed
        if converted > 0:
            r.conversion.was_converted = True
            r.conversion.original_filetype = original_ext or "flac"
            r.conversion.target_filetype = "mp3"
        _log(f"  Converted {converted}, failed {failed}")
        cd = conversion_decision(converted, failed)
        if cd.is_terminal:
            r.exit_code = cd.exit_code
            r.decision = cd.decision
            r.error = cd.error
            _log(f"[ERROR] {r.error}")
            _emit_and_exit(r)

        # --- Transcode detection ---
        # When keep_source=True, FLAC+MP3 coexist — measure only MP3 for V0 bitrate
        v0_ext_filter = {".mp3"} if args.opus_conversion and converted > 0 else None
        post_conv_br = _get_folder_min_bitrate(args.path, ext_filter=v0_ext_filter) if converted > 0 else None
        r.conversion.post_conversion_min_bitrate = post_conv_br
        is_transcode = transcode_detection(converted, post_conv_br,
                                           spectral_grade=spectral_grade)
        r.conversion.is_transcode = is_transcode
        if is_transcode:
            _log(f"[TRANSCODE] converted FLAC min bitrate {post_conv_br}kbps "
                 f"< {TRANSCODE_MIN_BITRATE_KBPS}kbps — source was not lossless")
        if post_conv_br is not None:
            _log(f"  post_conversion_min_bitrate={post_conv_br}")
    else:
        _log(f"[CONVERT] Skipping conversion (target_format={args.target_format})")
        r.final_format = args.target_format

    if args.dry_run:
        r.decision = "dry_run"
        _emit_and_exit(r)

    # --- Quality comparison ---
    new_min_br = _get_folder_min_bitrate(args.path, ext_filter=v0_ext_filter)
    existing_min_br = beets.get_min_bitrate(mbid)
    if args.override_min_bitrate is not None and existing_min_br is not None:
        if args.override_min_bitrate != existing_min_br:
            _log(f"  [OVERRIDE] pipeline says {args.override_min_bitrate}kbps, "
                 f"beets says {existing_min_br}kbps")
    effective_existing = args.override_min_bitrate if args.override_min_bitrate is not None else existing_min_br
    if effective_existing is not None:
        _log(f"  prev_min_bitrate={effective_existing}")
    if new_min_br is not None:
        _log(f"  new_min_bitrate={new_min_br}")

    # Verified lossless: genuine FLAC kept on disk counts as verified
    if args.target_format == "flac":
        will_be_verified_lossless = spectral_grade in ("genuine", "marginal", None)
    else:
        will_be_verified_lossless = (converted > 0 and not is_transcode)

    # --- Build measurements ---
    new_m = AudioQualityMeasurement(
        min_bitrate_kbps=new_min_br,
        spectral_grade=spectral_grade,
        spectral_bitrate_kbps=spectral_bitrate,
        verified_lossless=will_be_verified_lossless,
        was_converted_from=(original_ext or "flac") if converted > 0 else None,
    )
    existing_m = (AudioQualityMeasurement(
        min_bitrate_kbps=effective_existing,
        spectral_grade=existing_spectral_grade,
        spectral_bitrate_kbps=existing_spectral_bitrate,
    ) if existing_min_br is not None else None)
    r.new_measurement = new_m
    r.existing_measurement = existing_m

    # --- Quality comparison (pure decision) ---
    qd = quality_decision_stage(new_m, existing_m, is_transcode=is_transcode)
    decision = qd.decision
    r.decision = decision

    if qd.is_terminal:
        r.exit_code = qd.exit_code
        _log(f"[QUALITY DOWNGRADE] new {new_min_br}kbps <= existing "
             f"{effective_existing}kbps — skipping import"
             f"{' (transcode)' if decision == 'transcode_downgrade' else ''}")
        _emit_and_exit(r)

    # Non-terminal quality decisions — log and proceed to import
    if decision == "import":
        if will_be_verified_lossless and effective_existing is not None:
            _log(f"  [QUALITY] genuine FLAC→V0 at {new_min_br}kbps — "
                 f"always upgrade over existing {effective_existing}kbps")
        elif effective_existing is not None:
            _log(f"  [QUALITY] new {new_min_br}kbps > existing {effective_existing}kbps — upgrading")
    elif decision == "transcode_upgrade":
        _log(f"  [QUALITY] new {new_min_br}kbps > existing "
             f"{effective_existing}kbps — upgrading (transcode)")
    elif decision == "transcode_first":
        _log(f"  [QUALITY] no existing album in beets — importing transcode")

    # --- Opus conversion (after V0 verdict, before import) ---
    conv = conversion_target(args.target_format, will_be_verified_lossless, args.opus_conversion)
    if conv == "opus":
        _log(f"[OPUS] Converting verified lossless → Opus 128kbps")
        r.v0_verification_bitrate = post_conv_br
        opus_converted, opus_failed = convert_lossless_to_opus(args.path, dry_run=args.dry_run)
        if opus_failed > 0:
            r.exit_code = 1
            r.decision = "opus_conversion_failed"
            r.error = f"{opus_failed} Opus conversions failed"
            _log(f"[ERROR] {r.error}")
            _emit_and_exit(r)
        # Remove V0 temp files (they were ephemeral verification artifacts)
        for fname in os.listdir(args.path):
            if fname.lower().endswith(".mp3"):
                os.remove(os.path.join(args.path, fname))
        # Remove original lossless files
        for fname in os.listdir(args.path):
            if _is_lossless_file(fname, args.path):
                os.remove(os.path.join(args.path, fname))
        # Update measurements and conversion info for Opus
        opus_min_br = _get_folder_min_bitrate(args.path)
        r.new_measurement = AudioQualityMeasurement(
            min_bitrate_kbps=opus_min_br,
            spectral_grade=spectral_grade,
            spectral_bitrate_kbps=spectral_bitrate,
            verified_lossless=True,
            was_converted_from=(original_ext or "flac"),
        )
        r.conversion.target_filetype = "opus"
        r.conversion.final_format = "opus 128"
        r.final_format = "opus 128"
        _log(f"  Opus conversion complete: {opus_converted} files, min_bitrate={opus_min_br}kbps")
        _log(f"  V0 verification bitrate: {post_conv_br}kbps")

    # --- Clean up kept FLAC files if opus was skipped (transcode path) ---
    if opus_cleanup_decision(conv != "opus",
                             args.opus_conversion, converted):
        for fname in os.listdir(args.path):
            if _is_lossless_file(fname, args.path):
                os.remove(os.path.join(args.path, fname))
        _log(f"  [CLEANUP] Removed lossless originals (opus skipped, not verified lossless)")

    # --- Import ---
    _log(f"[IMPORT] {args.path} → beets (mbid={mbid})")
    rc, beets_lines, kept_duplicate = run_import(args.path, mbid)
    r.beets_log = beets_lines

    if rc != 0:
        r.exit_code = rc
        r.decision = "import_failed" if rc == 2 else "mbid_missing" if rc == 4 else "import_failed"
        r.error = next((line for line in reversed(beets_lines) if line.strip()),
                       f"Harness returned rc={rc}")
        _log(f"[ERROR] Import failed (rc={rc})")
        _emit_and_exit(r)

    # --- Post-flight verification ---
    pf_info = beets.get_album_info(mbid)
    if not pf_info:
        r.exit_code = 2
        r.decision = "import_failed"
        r.error = f"Post-flight: MBID {mbid} NOT in beets DB after import"
        _log(f"[ERROR] {r.error}")
        beets.close()
        _emit_and_exit(r)

    r.postflight = PostflightInfo(beets_id=pf_info.album_id,
                                   track_count=pf_info.track_count,
                                   imported_path=pf_info.album_path)
    album_path = pf_info.album_path
    _log(f"[POST-FLIGHT OK] mbid={mbid}, beets_id={pf_info.album_id}, "
         f"tracks={pf_info.track_count}, path={album_path}")

    # --- Post-import %aunique disambiguation ---
    # When beets kept a different edition during duplicate resolution,
    # %aunique doesn't fully disambiguate at import time (the new album
    # gets no disambiguator if its field value is empty). Running
    # `beet move` re-evaluates all editions and fixes the paths.
    if kept_duplicate:
        _log(f"[DISAMBIGUATE] Running beet move for album id:{pf_info.album_id}")
        move_result = subprocess.run(
            [BEET_BIN, "move", f"mb_albumid:{mbid}"],
            capture_output=True, text=True, timeout=120,
            env={**os.environ, "HOME": "/home/abl030"},
        )
        if move_result.returncode == 0:
            # Re-read path from beets DB — it may have changed
            pf_info_after = beets.get_album_info(mbid)
            if pf_info_after:
                new_path = pf_info_after.album_path
                if new_path != album_path:
                    _log(f"  [DISAMBIGUATE] Path changed: {album_path} → {new_path}")
                    album_path = new_path
                    r.postflight.imported_path = new_path
                else:
                    _log(f"  [DISAMBIGUATE] Path unchanged (already unique)")
            r.postflight.disambiguated = True
        else:
            _log(f"  [DISAMBIGUATE] beet move failed (rc={move_result.returncode}): "
                 f"{move_result.stderr[:200]}")

    # --- Post-import extension check ---
    # Detect .bak files (known bug: track 01 sometimes renamed to .bak during import)
    VALID_AUDIO_EXT = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wma", ".wav"}
    item_paths = beets.get_item_paths(mbid)
    bad_ext_files = []
    for item_id, item_path in item_paths:
        ext = os.path.splitext(item_path)[1].lower()
        if ext not in VALID_AUDIO_EXT and os.path.isfile(item_path):
            # Determine correct extension from actual audio format
            try:
                probe = subprocess.run(
                    ["ffprobe", "-v", "error", "-show_entries", "format=format_name",
                     "-of", "csv=p=0", item_path],
                    capture_output=True, text=True, timeout=15)
                fmt = probe.stdout.strip().split(",")[0] if probe.stdout.strip() else ""
                ext_map = {"mp3": ".mp3", "flac": ".flac", "ogg": ".ogg",
                           "opus": ".opus", "wav": ".wav", "mp4": ".m4a"}
                correct_ext = ext_map.get(fmt, ".mp3")
            except Exception:
                correct_ext = ".mp3"
            new_path = os.path.splitext(item_path)[0] + correct_ext
            _log(f"[EXT-FIX] {os.path.basename(item_path)} → {os.path.basename(new_path)}")
            os.rename(item_path, new_path)
            # Update beets DB (writable connection for this fix)
            import sqlite3 as _sqlite3
            from beets_db import DEFAULT_BEETS_DB
            with _sqlite3.connect(DEFAULT_BEETS_DB) as fix_conn:
                fix_conn.execute("UPDATE items SET path = ? WHERE id = ?",
                                 (new_path.encode(), item_id))
            bad_ext_files.append(os.path.basename(item_path))
    if bad_ext_files:
        r.postflight.bad_extensions = bad_ext_files
        _log(f"[EXT-FIX] Fixed {len(bad_ext_files)} file(s) with bad extensions")

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
        parent = os.path.dirname(args.path)
        try:
            os.rmdir(parent)
        except OSError:
            pass

    # --- Pipeline DB: imported ---
    if request_id:
        update_pipeline_db(request_id, "imported", imported_path=album_path)

    # --- Final exit ---
    beets.close()
    r.exit_code = final_exit_decision(is_transcode)
    if is_transcode:
        _log("[OK] Transcode imported (upgrade) — denylist user, keep searching")
    else:
        _log("[OK] Import complete")
    _emit_and_exit(r)


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise  # _emit_and_exit uses sys.exit
    except Exception as exc:
        # Preserve intermediate data if main() had started building a result
        if _current_result is not None:
            r = _current_result
            r.exit_code = 99
            r.decision = "crash"
            r.error = f"{type(exc).__name__}: {exc}"
        else:
            r = ImportResult(exit_code=99, decision="crash",
                             error=f"{type(exc).__name__}: {exc}")
        _emit_and_exit(r)
