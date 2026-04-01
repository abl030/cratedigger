#!/usr/bin/env python3
"""Scan a beets library directory for disambiguation orphans.

Walks every artist directory looking for album subdirectories that contain
no audio files (only clutter like cover.jpg, Thumbs.DB, .DS_Store).

Usage:
    python3 scripts/cleanup_orphans.py [--dry-run] [--path /mnt/virtio/Music/Beets]
"""

import argparse
import os
import shutil
import sys

_AUDIO_EXTS = {"mp3", "flac", "m4a", "ogg", "opus", "wma", "aac", "alac", "wav"}


def _has_audio(directory: str) -> bool:
    for f in os.listdir(directory):
        if os.path.isfile(os.path.join(directory, f)) and "." in f:
            if f.rsplit(".", 1)[-1].lower() in _AUDIO_EXTS:
                return True
    return False


def scan(beets_dir: str, dry_run: bool) -> list[str]:
    orphans: list[str] = []
    for artist in sorted(os.listdir(beets_dir)):
        artist_path = os.path.join(beets_dir, artist)
        if not os.path.isdir(artist_path):
            continue
        for album in sorted(os.listdir(artist_path)):
            album_path = os.path.join(artist_path, album)
            if not os.path.isdir(album_path):
                continue
            if not _has_audio(album_path):
                contents = os.listdir(album_path)
                orphans.append(album_path)
                if dry_run:
                    print(f"[DRY RUN] would remove: {album_path}  ({contents})")
                else:
                    shutil.rmtree(album_path)
                    print(f"removed: {album_path}  ({contents})")
    return orphans


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove disambiguation orphan directories")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be removed")
    parser.add_argument("--path", default="/mnt/virtio/Music/Beets",
                        help="Beets library root (default: /mnt/virtio/Music/Beets)")
    args = parser.parse_args()

    if not os.path.isdir(args.path):
        print(f"error: {args.path} is not a directory", file=sys.stderr)
        sys.exit(1)

    orphans = scan(args.path, args.dry_run)
    print(f"\n{len(orphans)} orphan(s) found")


if __name__ == "__main__":
    main()
