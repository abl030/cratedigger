#!/usr/bin/env python3
"""Fix .bak files in the beets library.

Finds all items in beets with non-audio extensions (.bak, .tmp, etc),
renames them to the correct extension based on ffprobe format detection,
and updates the beets DB paths.

Usage:
    python3 scripts/fix_bak_files.py [--dry-run] [path_filter]

    path_filter: only fix items whose path contains this string
    Examples:
        python3 scripts/fix_bak_files.py --dry-run "PJ Harvey"
        python3 scripts/fix_bak_files.py "PJ Harvey"
        python3 scripts/fix_bak_files.py --dry-run
"""

import json
import os
import sqlite3
import subprocess
import sys

BEETS_DB = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")
VALID_AUDIO_EXT = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wma", ".wav"}
CODEC_EXT_MAP = {
    "mp3": ".mp3",
    "flac": ".flac",
    "opus": ".opus",
    "vorbis": ".ogg",
    "aac": ".m4a",
    "alac": ".m4a",
    "wav": ".wav",
    "pcm_s16le": ".wav",
    "pcm_s24le": ".wav",
    "pcm_s32le": ".wav",
    "wmav1": ".wma",
    "wmav2": ".wma",
    "wmalossless": ".wma",
}
FORMAT_EXT_MAP = {
    "mp3": ".mp3",
    "flac": ".flac",
    "ogg": ".ogg",
    "wav": ".wav",
    "asf": ".wma",
    "mp4": ".m4a",
    "mov": ".m4a",
    "m4a": ".m4a",
}


def detect_format(path: str) -> str | None:
    """Detect audio format via ffprobe. Returns extension like '.mp3'.

    Prefer the audio stream codec over the container name. Containers are
    ambiguous: Ogg can carry Opus or Vorbis, and MP4/M4A can carry AAC or
    ALAC. Unknown probes return None so the manual repair never guesses MP3.
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error",
             "-show_entries", "stream=codec_type,codec_name:format=format_name",
             "-of", "json", path],
            capture_output=True, text=True, timeout=15)
        if result.returncode != 0:
            return None
        payload = json.loads(result.stdout or "{}")
        streams = payload.get("streams")
        if isinstance(streams, list):
            for stream in streams:
                if not isinstance(stream, dict):
                    continue
                if stream.get("codec_type") != "audio":
                    continue
                codec = str(stream.get("codec_name") or "").lower()
                if codec in CODEC_EXT_MAP:
                    return CODEC_EXT_MAP[codec]
        fmt_obj = payload.get("format")
        if isinstance(fmt_obj, dict):
            names = str(fmt_obj.get("format_name") or "").lower().split(",")
            for name in names:
                if name in FORMAT_EXT_MAP:
                    return FORMAT_EXT_MAP[name]
        return None
    except (json.JSONDecodeError, OSError, subprocess.TimeoutExpired):
        return None


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    # Filter: only fix items whose path contains this string
    path_filter = None
    for arg in sys.argv[1:]:
        if arg != "--dry-run":
            path_filter = arg
            break

    if not os.path.exists(BEETS_DB):
        print(f"ERROR: Beets DB not found: {BEETS_DB}")
        sys.exit(1)

    conn = sqlite3.connect(BEETS_DB)
    rows = conn.execute("SELECT id, path FROM items").fetchall()

    bad = []
    for item_id, raw_path in rows:
        path = raw_path.decode("utf-8", errors="replace") if isinstance(raw_path, bytes) else raw_path
        if path_filter and path_filter not in path:
            continue
        ext = os.path.splitext(path)[1].lower()
        if ext not in VALID_AUDIO_EXT:
            bad.append((item_id, path))

    if not bad:
        print("No files with bad extensions found.")
        conn.close()
        return

    print(f"Found {len(bad)} file(s) with bad extensions:\n")

    fixed = 0
    errors = 0
    for item_id, path in bad:
        if not os.path.isfile(path):
            print(f"  MISSING: {path}")
            errors += 1
            continue

        correct_ext = detect_format(path)
        if correct_ext is None:
            print(f"  UNKNOWN: could not detect audio codec, skipping {os.path.basename(path)}")
            errors += 1
            continue
        new_path = os.path.splitext(path)[0] + correct_ext

        if os.path.exists(new_path):
            print(f"  CONFLICT: {os.path.basename(new_path)} already exists, skipping {os.path.basename(path)}")
            errors += 1
            continue

        if dry_run:
            print(f"  [DRY] {os.path.basename(path)} → {os.path.basename(new_path)}")
        else:
            os.rename(path, new_path)
            conn.execute("UPDATE items SET path = ? WHERE id = ?",
                         (new_path.encode(), item_id))
            print(f"  FIXED: {os.path.basename(path)} → {os.path.basename(new_path)}")
        fixed += 1

    if not dry_run:
        conn.commit()
    conn.close()

    print(f"\n{'Would fix' if dry_run else 'Fixed'}: {fixed}, Errors: {errors}")


if __name__ == "__main__":
    main()
