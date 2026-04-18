#!/usr/bin/env python3
"""Populate album_tracks from MusicBrainz API for all requests missing tracks.

Reads the pipeline DB, finds requests with an mb_release_id but no tracks,
fetches the release from the local MB mirror, extracts tracks, and stores them.

Rate limit: 1 request per 100ms (local mirror allows ratelimit 100).
"""

import sys
import os
import time

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
from lib.pipeline_db import PipelineDB
from scripts.pipeline_cli import fetch_mb_release, tracks_from_mb_release

DB_PATH = "/mnt/virtio/Music/pipeline.db"
RATE_LIMIT_SECONDS = 0.1  # 100ms between requests


def main():
    db = PipelineDB(DB_PATH)

    # Get all requests
    rows = db._execute(
        "SELECT id, mb_release_id, artist_name, album_title FROM album_requests ORDER BY id"
    ).fetchall()

    # Filter to those with mb_release_id and no tracks
    needs_tracks = []
    for r in rows:
        r = dict(r)
        if not r["mb_release_id"]:
            continue
        if not db.get_tracks(r["id"]):
            needs_tracks.append(r)

    total = len(needs_tracks)
    if total == 0:
        print("All requests already have tracks. Nothing to do.")
        db.close()
        return

    print(f"Found {total} requests needing tracks.\n")

    populated = 0
    failed = 0

    for i, req in enumerate(needs_tracks, 1):
        rid = req["id"]
        mbid = req["mb_release_id"]
        label = f"{req['artist_name']} - {req['album_title']}"

        print(f"[{i}/{total}] id={rid} {label} ...", end=" ", flush=True)

        release_data = fetch_mb_release(mbid)
        if not release_data:
            print("FAILED (API error)")
            failed += 1
            time.sleep(RATE_LIMIT_SECONDS)
            continue

        tracks = tracks_from_mb_release(release_data)
        if not tracks:
            print("FAILED (no tracks in response)")
            failed += 1
            time.sleep(RATE_LIMIT_SECONDS)
            continue

        db.set_tracks(rid, tracks)
        populated += 1
        print(f"OK ({len(tracks)} tracks)")

        time.sleep(RATE_LIMIT_SECONDS)

    print(f"\nDone. Populated: {populated}, Failed: {failed}, Total: {total}")
    db.close()


if __name__ == "__main__":
    main()
