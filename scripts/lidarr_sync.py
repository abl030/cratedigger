#!/usr/bin/env python3
"""Lidarr sync bridge — import Lidarr's monitored albums into pipeline DB.

Lidarr is used as a mobile-friendly album picker — the pipeline DB is the SSOT.
On each sync: pull monitored+missing albums → add to pipeline DB → unmonitor in Lidarr.

Usage:
    python3 scripts/lidarr_sync.py                  # one-shot sync
    python3 scripts/lidarr_sync.py --dry-run         # preview without changes
    python3 scripts/lidarr_sync.py --watch            # poll every 5 min
    python3 scripts/lidarr_sync.py --reset-all        # unmonitor ALL albums in Lidarr
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.dirname(__file__))
from pipeline_db import PipelineDB, DEFAULT_DSN

LIDARR_URL = os.environ.get("LIDARR_URL", "https://lidarr.ablz.au")
LIDARR_KEY_FILE = os.path.join(os.path.dirname(__file__), "..", "secrets", "lidarr-api-key")
MB_API = "http://192.168.1.35:5200/ws/2"


def get_lidarr_api_key():
    # Environment variable takes priority (used by Soularr pre-start on doc2)
    key = os.environ.get("LIDARR_API_KEY")
    if key:
        return key
    with open(LIDARR_KEY_FILE) as f:
        return f.read().strip()


def lidarr_get(endpoint, api_key, params=None):
    """GET from Lidarr API."""
    url = f"{LIDARR_URL}/api/v1/{endpoint}"
    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        url += f"?{qs}"
    req = urllib.request.Request(url)
    req.add_header("X-Api-Key", api_key)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def lidarr_put(endpoint, api_key, data):
    """PUT to Lidarr API."""
    url = f"{LIDARR_URL}/api/v1/{endpoint}"
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="PUT")
    req.add_header("X-Api-Key", api_key)
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def get_wanted_albums(api_key, missing=True):
    """Fetch all wanted (missing or cutoff_unmet) albums from Lidarr."""
    albums = []
    page = 1
    while True:
        data = lidarr_get("wanted/missing" if missing else "wanted/cutoff", api_key, {
            "page": page,
            "pageSize": 50,
            "sortKey": "albums.title",
            "sortDirection": "ascending",
        })
        albums.extend(data.get("records", []))
        if len(albums) >= data.get("totalRecords", 0):
            break
        page += 1
    return albums


def extract_monitored_release(album):
    """Get the monitored release's MB release ID from a Lidarr album record."""
    for release in album.get("releases", []):
        if release.get("monitored", False):
            return release.get("foreignReleaseId")
    return None


def unmonitor_album(api_key, album):
    """Unmonitor an album in Lidarr so it doesn't show up as wanted again."""
    album["monitored"] = False
    try:
        lidarr_put(f"album/{album['id']}", api_key, album)
    except Exception as e:
        print(f"  [WARN] Failed to unmonitor album {album['id']}: {e}", file=sys.stderr)


def unmonitor_all_albums(api_key, dry_run=False):
    """Unmonitor every album in Lidarr. One-off reset."""
    print("Fetching all albums from Lidarr...")
    albums = lidarr_get("album", api_key)
    monitored = [a for a in albums if a.get("monitored", False)]
    print(f"  {len(monitored)} monitored albums out of {len(albums)} total")

    if dry_run:
        for a in monitored[:10]:
            artist = a.get("artist", {}).get("artistName", "?")
            print(f"  [DRY] Would unmonitor: {artist} - {a.get('title', '?')}")
        if len(monitored) > 10:
            print(f"  ... and {len(monitored) - 10} more")
        return

    count = 0
    for a in monitored:
        a["monitored"] = False
        try:
            lidarr_put(f"album/{a['id']}", api_key, a)
            count += 1
        except Exception as e:
            artist = a.get("artist", {}).get("artistName", "?")
            print(f"  [WARN] Failed to unmonitor {artist} - {a.get('title', '?')}: {e}",
                  file=sys.stderr)

    print(f"  Unmonitored {count} albums")


def fetch_mb_tracks(mb_release_id):
    """Fetch tracks from MB API for a release."""
    url = f"{MB_API}/release/{mb_release_id}?inc=recordings&fmt=json"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "lidarr-sync/1.0")
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
    except Exception:
        return []

    tracks = []
    for medium in data.get("media", []):
        disc = medium.get("position", 1)
        for track in medium.get("tracks", []):
            length_ms = track.get("length") or (track.get("recording") or {}).get("length")
            tracks.append({
                "disc_number": disc,
                "track_number": track.get("position", track.get("number", 0)),
                "title": track.get("title", ""),
                "length_seconds": round(length_ms / 1000, 1) if length_ms else None,
            })
    return tracks


def sync_once(db, api_key, dry_run=False):
    """Run one sync cycle."""
    print("Fetching wanted albums from Lidarr...")
    missing = get_wanted_albums(api_key, missing=True)
    cutoff = get_wanted_albums(api_key, missing=False)

    # Combine and deduplicate by album ID
    seen = set()
    all_albums = []
    for album in missing + cutoff:
        if album["id"] not in seen:
            seen.add(album["id"])
            all_albums.append(album)

    print(f"  Found {len(missing)} missing + {len(cutoff)} cutoff = {len(all_albums)} unique albums")

    added = 0
    skipped = 0
    for album in all_albums:
        mb_release_id = extract_monitored_release(album)
        if not mb_release_id:
            continue

        # Skip if already in DB — but unmonitor in Lidarr if still monitored
        existing = db.get_request_by_mb_release_id(mb_release_id)
        if existing:
            if not dry_run and album.get("monitored", False):
                unmonitor_album(api_key, album)
            skipped += 1
            continue

        artist_name = album.get("artist", {}).get("artistName", "Unknown")
        album_title = album.get("title", "Unknown")
        year = None
        if album.get("releaseDate"):
            year = int(album["releaseDate"][:4]) if len(album["releaseDate"]) >= 4 else None

        if dry_run:
            print(f"  [DRY] Would add: {artist_name} - {album_title} ({mb_release_id})")
            added += 1
            continue

        req_id = db.add_request(
            mb_release_id=mb_release_id,
            mb_release_group_id=album.get("foreignAlbumId"),
            mb_artist_id=album.get("artist", {}).get("foreignArtistId"),
            artist_name=artist_name,
            album_title=album_title,
            year=year,
            source="request",
            lidarr_album_id=album.get("id"),
            lidarr_artist_id=album.get("artistId"),
        )

        # Populate tracks from MB API
        tracks = fetch_mb_tracks(mb_release_id)
        if tracks:
            db.set_tracks(req_id, tracks)

        # Unmonitor in Lidarr — pipeline DB is now the SSOT
        if not dry_run:
            unmonitor_album(api_key, album)

        added += 1
        print(f"  [ADD] {artist_name} - {album_title} ({len(tracks)} tracks)")

    print(f"\nSync complete: {added} added, {skipped} already in DB")
    return added


def main():
    parser = argparse.ArgumentParser(description="Sync Lidarr wanted albums to pipeline DB")
    parser.add_argument("--dsn", default=DEFAULT_DSN, help="PostgreSQL connection string")
    parser.add_argument("--dry-run", action="store_true", help="Preview without changes")
    parser.add_argument("--watch", action="store_true", help="Poll every 5 minutes")
    parser.add_argument("--reset-all", action="store_true",
                        help="Unmonitor ALL albums in Lidarr (one-off reset)")
    args = parser.parse_args()

    api_key = get_lidarr_api_key()

    if args.reset_all:
        unmonitor_all_albums(api_key, dry_run=args.dry_run)
        return

    db = PipelineDB(args.dsn)

    if args.watch:
        print("Watching Lidarr (polling every 5 min)...")
        while True:
            try:
                sync_once(db, api_key, dry_run=args.dry_run)
            except Exception as e:
                print(f"  [ERROR] {e}", file=sys.stderr)
            time.sleep(300)
    else:
        sync_once(db, api_key, dry_run=args.dry_run)

    db.close()


if __name__ == "__main__":
    main()
