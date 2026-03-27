#!/usr/bin/env python3
"""Pipeline CLI — manage the download pipeline database.

Commands:
    list [status]       List album requests (optionally filtered by status)
    add <mbid>          Add a new request by MusicBrainz release ID
    status              Show counts by status
    retry <id>          Reset a failed/rejected request to wanted
    cancel <id>         Set a request to skipped
    set <id> <status>   Change status (wanted, imported, manual)
    show <id>           Show full details of a request

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
import sys
import urllib.request
import urllib.error

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
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
    for status in ["wanted", "imported", "manual"]:
        c = counts.get(status, 0)
        if c > 0:
            print(f"    {status:15s} {c:4d}")


def cmd_retry(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    db.reset_to_wanted(args.id)
    print(f"  Reset to wanted: [{args.id}] {req['artist_name']} - {req['album_title']}")


def cmd_cancel(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    db.update_status(args.id, "manual")
    print(f"  Marked for manual download: [{args.id}] {req['artist_name']} - {req['album_title']}")


VALID_STATUSES = ["wanted", "imported", "manual"]


def cmd_set(db, args):
    req = db.get_request(args.id)
    if not req:
        print(f"  Request {args.id} not found.")
        return
    old_status = req['status']
    if old_status == args.status:
        print(f"  [{args.id}] already has status '{args.status}'.")
        return
    db.update_status(args.id, args.status)
    print(f"  [{args.id}] {req['artist_name']} - {req['album_title']}: {old_status} → {args.status}")


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
    if req['reasoning']:
        print(f"  Reasoning:    {req['reasoning'][:120]}...")
    print(f"  Distance:     {req['beets_distance']}")
    print(f"  Imported:     {req['imported_path']}")
    print(f"  Lidarr:       artist={req['lidarr_artist_id']} album={req['lidarr_album_id']}")
    print(f"  Attempts:     search={req['search_attempts']} dl={req['download_attempts']} val={req['validation_attempts']}")
    print(f"  Created:      {req['created_at']}")
    print(f"  Updated:      {req['updated_at']}")

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

    denied = db.get_denylisted_users(req['id'])
    if denied:
        print(f"\n  Denylisted Users ({len(denied)}):")
        for d in denied:
            print(f"    {d['username']}: {d['reason']}")



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

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    db = PipelineDB(args.dsn, run_migrations=True)

    commands = {
        "list": cmd_list,
        "add": cmd_add,
        "status": cmd_status,
        "retry": cmd_retry,
        "cancel": cmd_cancel,
        "set": cmd_set,
        "show": cmd_show,
    }
    commands[args.command](db, args)
    db.close()


if __name__ == "__main__":
    main()
