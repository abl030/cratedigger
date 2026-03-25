#!/usr/bin/env python3
"""Soularr Web UI — album request manager at music.ablz.au.

Browse MusicBrainz, add releases to the pipeline DB, view status.

Usage:
    python3 web/server.py --port 8085 --dsn postgresql://soularr@192.168.100.11/soularr
"""

import argparse
import json
import os
import re
import sqlite3
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.dirname(__file__))

import mb as mb_api
from pipeline_db import PipelineDB

# Globals set in main()
db = None
beets_db_path = None


def check_beets_library(mbids):
    """Check which MBIDs are already in the beets library. Returns set of found MBIDs."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return set()
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    placeholders = ",".join("?" for _ in mbids)
    rows = conn.execute(
        f"SELECT mb_albumid FROM albums WHERE mb_albumid IN ({placeholders})", mbids
    ).fetchall()
    conn.close()
    return {r[0] for r in rows}


def get_library_artist(artist_name):
    """Get albums by an artist from the beets library."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return []
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT album, albumartist, year, mb_albumid, disc_total, "
        "       (SELECT COUNT(*) FROM items WHERE items.album_id = albums.id) as track_count "
        "FROM albums WHERE albumartist LIKE ? COLLATE NOCASE "
        "ORDER BY year, album",
        (f"%{artist_name}%",),
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        # Check for discogs — beets stores it in the 'data_source' flexible attr
        # or we can check if mb_albumid is empty
        has_mb = bool(r[3])
        results.append({
            "album": r[0],
            "albumartist": r[1],
            "year": r[2],
            "mb_albumid": r[3],
            "track_count": r[5],
            "source": "musicbrainz" if has_mb else "discogs",
        })
    return results


def check_pipeline(mbids):
    """Check which MBIDs are already in the pipeline DB. Returns dict of mbid → status."""
    result = {}
    for mbid in mbids:
        req = db.get_request_by_mb_release_id(mbid)
        if req:
            result[mbid] = req["status"]
    return result


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        # Quieter logging
        pass

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, path):
        html_path = os.path.join(os.path.dirname(__file__), path)
        with open(html_path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, msg, status=400):
        self._json({"error": msg}, status)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

        try:
            if path == "/":
                self._html("index.html")

            elif path == "/api/search":
                q = params.get("q", [""])[0].strip()
                if not q:
                    self._error("Missing query parameter 'q'")
                    return
                artists = mb_api.search_artists(q)
                self._json({"artists": artists})

            elif path == "/api/library/artist":
                name = params.get("name", [""])[0].strip()
                if not name:
                    self._error("Missing parameter 'name'")
                    return
                albums = get_library_artist(name)
                self._json({"albums": albums})

            elif re.match(r"^/api/artist/[a-f0-9-]+$", path):
                artist_id = path.split("/")[-1]
                rgs = mb_api.get_artist_release_groups(artist_id)
                self._json({"release_groups": rgs})

            elif re.match(r"^/api/release-group/[a-f0-9-]+$", path):
                rg_id = path.split("/")[-1]
                data = mb_api.get_release_group_releases(rg_id)
                # Check which releases are in pipeline/library
                mbids = [r["id"] for r in data["releases"]]
                in_library = check_beets_library(mbids)
                in_pipeline = check_pipeline(mbids)
                for r in data["releases"]:
                    r["in_library"] = r["id"] in in_library
                    r["pipeline_status"] = in_pipeline.get(r["id"])
                self._json(data)

            elif re.match(r"^/api/release/[a-f0-9-]+$", path):
                release_id = path.split("/")[-1]
                data = mb_api.get_release(release_id)
                data["in_library"] = bool(check_beets_library([release_id]))
                req = db.get_request_by_mb_release_id(release_id)
                data["pipeline_status"] = req["status"] if req else None
                data["pipeline_id"] = req["id"] if req else None
                self._json(data)

            elif path == "/api/pipeline/status":
                counts = db.count_by_status()
                wanted = db.get_wanted(limit=50)
                self._json({
                    "counts": counts,
                    "wanted": [
                        {
                            "id": w["id"],
                            "artist": w["artist_name"],
                            "album": w["album_title"],
                            "mb_release_id": w["mb_release_id"],
                            "source": w["source"],
                            "created_at": str(w["created_at"]),
                        }
                        for w in wanted
                    ],
                })

            elif re.match(r"^/api/pipeline/\d+$", path):
                req_id = int(path.split("/")[-1])
                req = db.get_request(req_id)
                if not req:
                    self._error("Not found", 404)
                    return
                tracks = db.get_tracks(req_id)
                history = db.get_download_history(req_id)
                self._json({
                    "request": {k: str(v) if hasattr(v, 'isoformat') else v for k, v in req.items()},
                    "tracks": tracks,
                    "history": [{k: str(v) if hasattr(v, 'isoformat') else v for k, v in h.items()} for h in history],
                })

            else:
                self._error("Not found", 404)

        except Exception as e:
            self._error(str(e), 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            if path == "/api/pipeline/add":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                mbid = body.get("mb_release_id", "").strip()
                source = body.get("source", "request")

                if not mbid:
                    self._error("Missing mb_release_id")
                    return

                # Check if already exists
                existing = db.get_request_by_mb_release_id(mbid)
                if existing:
                    self._json({
                        "status": "exists",
                        "id": existing["id"],
                        "current_status": existing["status"],
                    })
                    return

                # Fetch from MB
                release = mb_api.get_release(mbid)

                req_id = db.add_request(
                    mb_release_id=mbid,
                    mb_release_group_id=release.get("release_group_id"),
                    mb_artist_id=release.get("artist_id"),
                    artist_name=release["artist_name"],
                    album_title=release["title"],
                    year=release.get("year"),
                    country=release.get("country"),
                    source=source,
                )

                # Populate tracks
                if release.get("tracks"):
                    db.set_tracks(req_id, release["tracks"])

                self._json({
                    "status": "added",
                    "id": req_id,
                    "artist": release["artist_name"],
                    "album": release["title"],
                    "tracks": len(release.get("tracks", [])),
                })

            else:
                self._error("Not found", 404)

        except Exception as e:
            self._error(str(e), 500)

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()


def main():
    global db, beets_db_path

    parser = argparse.ArgumentParser(description="Soularr Web UI")
    parser.add_argument("--port", type=int, default=8085)
    parser.add_argument("--dsn", default=os.environ.get("PIPELINE_DB_DSN", "postgresql://soularr@localhost/soularr"))
    parser.add_argument("--beets-db", default="/mnt/virtio/Music/beets-library.db")
    parser.add_argument("--mb-api", default=None, help="MusicBrainz API base URL")
    args = parser.parse_args()

    if args.mb_api:
        mb_api.MB_API_BASE = args.mb_api

    db = PipelineDB(args.dsn)
    beets_db_path = args.beets_db

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Soularr Web UI listening on http://0.0.0.0:{args.port}")
    print(f"  Pipeline DB: {args.dsn}")
    print(f"  Beets DB: {beets_db_path}")
    print(f"  MB API: {mb_api.MB_API_BASE}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
    db.close()


if __name__ == "__main__":
    main()
