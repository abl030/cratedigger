#!/usr/bin/env python3
"""Fix .bak files in the beets library.

Finds all items in beets with non-audio extensions (.bak, .tmp, etc),
renames them to the correct extension based on ffprobe format detection,
and updates the beets DB paths.

Usage:
    python3 scripts/fix_bak_files.py [--dry-run]
"""

import os
import sqlite3
import subprocess
import sys

BEETS_DB = os.environ.get("BEETS_DB", "/mnt/virtio/Music/beets-library.db")
VALID_AUDIO_EXT = {".mp3", ".flac", ".ogg", ".opus", ".m4a", ".aac", ".wma", ".wav"}
EXT_MAP = {"mp3": ".mp3", "flac": ".flac", "ogg": ".ogg",
           "opus": ".opus", "wav": ".wav", "mp4": ".m4a"}


def detect_format(path: str) -> str:
    """Detect audio format via ffprobe. Returns extension like '.mp3'."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=format_name",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=15)
        fmt = result.stdout.strip().split(",")[0] if result.stdout.strip() else ""
        return EXT_MAP.get(fmt, ".mp3")
    except Exception:
        return ".mp3"


def main() -> None:
    dry_run = "--dry-run" in sys.argv

    if not os.path.exists(BEETS_DB):
        print(f"ERROR: Beets DB not found: {BEETS_DB}")
        sys.exit(1)

    conn = sqlite3.connect(BEETS_DB)
    rows = conn.execute("SELECT id, path FROM items").fetchall()

    bad = []
    for item_id, raw_path in rows:
        path = raw_path.decode("utf-8", errors="replace") if isinstance(raw_path, bytes) else raw_path
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
