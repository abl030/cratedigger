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
import shutil
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


def check_beets_library_detail(mbids):
    """Check beets library with track counts. Returns dict of mbid → {track_count}."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return {}
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    placeholders = ",".join("?" for _ in mbids)
    rows = conn.execute(
        f"SELECT a.mb_albumid, "
        f"       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count "
        f"FROM albums a WHERE a.mb_albumid IN ({placeholders})", mbids
    ).fetchall()
    conn.close()
    return {r[0]: {"beets_tracks": r[1]} for r in rows}


def check_beets_by_artist_album(artist, album):
    """Fuzzy check: is there an album by this artist in beets? Returns track count or None."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return None
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count "
        "FROM albums a WHERE a.albumartist LIKE ? COLLATE NOCASE "
        "AND a.album LIKE ? COLLATE NOCASE LIMIT 1",
        (f"%{artist}%", f"%{album}%"),
    ).fetchall()
    conn.close()
    return rows[0][0] if rows else None


def get_library_artist(artist_name):
    """Get albums by an artist from the beets library."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return []
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT album, albumartist, year, mb_albumid, discogs_albumid, "
        "       (SELECT COUNT(*) FROM items WHERE items.album_id = albums.id) as track_count "
        "FROM albums WHERE albumartist LIKE ? COLLATE NOCASE "
        "ORDER BY year, album",
        (f"%{artist_name}%",),
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        mb_id = r[3] or ""
        has_mb = bool(mb_id) and "-" in mb_id  # MB UUIDs have hyphens, Discogs IDs are numeric
        has_discogs = bool(r[4]) or (bool(mb_id) and "-" not in mb_id)
        source = "musicbrainz" if has_mb else ("discogs" if has_discogs else "unknown")
        results.append({
            "album": r[0],
            "albumartist": r[1],
            "year": r[2],
            "mb_albumid": r[3],
            "track_count": r[5],
            "source": source,
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
                official_rg_ids = mb_api.get_official_release_group_ids(artist_id)
                for rg in rgs:
                    rg["has_official"] = rg["id"] in official_rg_ids
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

            elif path == "/api/pipeline/recent":
                def serialize_req(r):
                    return {
                        k: str(v) if hasattr(v, 'isoformat') else v
                        for k, v in r.items()
                    }
                recent = db.get_recent(limit=20)
                # Check beets library for each and get track counts
                mbids = [r["mb_release_id"] for r in recent if r.get("mb_release_id")]
                beets_info = check_beets_library_detail(mbids) if mbids else {}
                serialized = []
                for r in recent:
                    item = serialize_req(r)
                    mbid = r.get("mb_release_id")
                    pipeline_tracks = len(db.get_tracks(r["id"]))
                    item["pipeline_tracks"] = pipeline_tracks
                    if mbid and mbid in beets_info:
                        item["in_beets"] = True
                        item["beets_tracks"] = beets_info[mbid]["beets_tracks"]
                    else:
                        # Fallback: match by artist + album name (different pressing)
                        fallback = check_beets_by_artist_album(
                            r.get("artist_name", ""), r.get("album_title", "")
                        )
                        if fallback is not None:
                            item["in_beets"] = True
                            item["beets_tracks"] = fallback
                        else:
                            item["in_beets"] = False
                            item["beets_tracks"] = 0
                    serialized.append(item)
                self._json({"recent": serialized})

            elif path == "/api/pipeline/all":
                def serialize_request(r):
                    return {
                        k: str(v) if hasattr(v, 'isoformat') else v
                        for k, v in r.items()
                    }
                counts = db.count_by_status()
                result = {"counts": counts}
                for status in ("wanted", "imported", "manual"):
                    result[status] = [serialize_request(r) for r in db.get_by_status(status)]
                self._json(result)

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

            elif path == "/api/beets/search":
                q = params.get("q", [""])[0].strip()
                if not q or len(q) < 2:
                    self._error("Query too short")
                    return
                if not beets_db_path or not os.path.exists(beets_db_path):
                    self._error("Beets DB not available")
                    return
                conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                rows = conn.execute(
                    "SELECT a.id, a.album, a.albumartist, a.year, a.mb_albumid, "
                    "       a.albumtype, a.label, a.country, "
                    "       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count, "
                    "       (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) as formats, "
                    "       a.added "
                    "FROM albums a "
                    "WHERE a.albumartist LIKE ? COLLATE NOCASE OR a.album LIKE ? COLLATE NOCASE "
                    "ORDER BY a.albumartist, a.year, a.album LIMIT 100",
                    (f"%{q}%", f"%{q}%"),
                ).fetchall()
                conn.close()
                albums = [{
                    "id": r[0], "album": r[1], "artist": r[2], "year": r[3],
                    "mb_albumid": r[4], "type": r[5], "label": r[6],
                    "country": r[7], "track_count": r[8],
                    "formats": r[9], "added": r[10],
                } for r in rows]
                self._json({"albums": albums})

            elif re.match(r"^/api/beets/album/\d+$", path):
                album_id = int(path.split("/")[-1])
                if not beets_db_path or not os.path.exists(beets_db_path):
                    self._error("Beets DB not available")
                    return
                conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                album = conn.execute(
                    "SELECT id, album, albumartist, year, mb_albumid, albumtype, "
                    "       label, country, artpath, added "
                    "FROM albums WHERE id = ?", (album_id,)
                ).fetchone()
                if not album:
                    conn.close()
                    self._error("Not found", 404)
                    return
                items = conn.execute(
                    "SELECT id, title, artist, track, disc, length, format, "
                    "       bitrate, samplerate, bitdepth, path "
                    "FROM items WHERE album_id = ? ORDER BY disc, track", (album_id,)
                ).fetchall()
                conn.close()
                tracks = [{
                    "id": i[0], "title": i[1], "artist": i[2], "track": i[3],
                    "disc": i[4], "length": i[5], "format": i[6],
                    "bitrate": i[7], "samplerate": i[8], "bitdepth": i[9],
                    "path": i[10].decode("utf-8", errors="replace") if isinstance(i[10], bytes) else i[10],
                } for i in items]
                # Derive album directory from first track path
                album_path = os.path.dirname(tracks[0]["path"]) if tracks else None
                self._json({
                    "id": album[0], "album": album[1], "artist": album[2],
                    "year": album[3], "mb_albumid": album[4], "type": album[5],
                    "label": album[6], "country": album[7],
                    "artpath": album[8].decode("utf-8", errors="replace") if isinstance(album[8], bytes) else album[8],
                    "added": album[9], "tracks": tracks, "path": album_path,
                })

            elif path == "/api/beets/recent":
                if not beets_db_path or not os.path.exists(beets_db_path):
                    self._error("Beets DB not available")
                    return
                conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                rows = conn.execute(
                    "SELECT a.id, a.album, a.albumartist, a.year, a.mb_albumid, "
                    "       a.albumtype, a.country, "
                    "       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count, "
                    "       (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) as formats, "
                    "       a.added "
                    "FROM albums a ORDER BY a.added DESC LIMIT 50",
                ).fetchall()
                conn.close()
                albums = [{
                    "id": r[0], "album": r[1], "artist": r[2], "year": r[3],
                    "mb_albumid": r[4], "type": r[5], "country": r[6],
                    "track_count": r[7], "formats": r[8], "added": r[9],
                } for r in rows]
                self._json({"albums": albums})

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

            elif path == "/api/pipeline/update":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                req_id = body.get("id")
                new_status = body.get("status", "").strip()

                if not req_id or not new_status:
                    self._error("Missing id or status")
                    return
                if new_status not in ("wanted", "imported", "manual"):
                    self._error(f"Invalid status: {new_status}")
                    return

                req = db.get_request(int(req_id))
                if not req:
                    self._error("Not found", 404)
                    return

                if new_status == "wanted" and req["status"] != "wanted":
                    db.reset_to_wanted(int(req_id))
                else:
                    db.update_status(int(req_id), new_status)

                self._json({"status": "ok", "id": req_id, "new_status": new_status})

            elif path == "/api/pipeline/delete":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                req_id = body.get("id")
                if not req_id:
                    self._error("Missing id")
                    return
                req = db.get_request(int(req_id))
                if not req:
                    self._error("Not found", 404)
                    return
                db.delete_request(int(req_id))
                self._json({"status": "ok", "id": req_id})

            elif path == "/api/beets/delete":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                album_id = body.get("id")
                confirm = body.get("confirm")
                if not album_id:
                    self._error("Missing id")
                    return
                if confirm != "DELETE":
                    self._error("Must send confirm: 'DELETE'")
                    return
                if not beets_db_path or not os.path.exists(beets_db_path):
                    self._error("Beets DB not available")
                    return
                conn = sqlite3.connect(beets_db_path)
                # Get album path from items before deleting
                items = conn.execute(
                    "SELECT path FROM items WHERE album_id = ?", (int(album_id),)
                ).fetchall()
                album_row = conn.execute(
                    "SELECT album, albumartist FROM albums WHERE id = ?", (int(album_id),)
                ).fetchone()
                if not album_row:
                    conn.close()
                    self._error("Album not found", 404)
                    return
                album_name = album_row[0]
                artist_name = album_row[1]
                # Get album directory from first track
                file_paths = [r[0].decode("utf-8", errors="replace") if isinstance(r[0], bytes) else r[0] for r in items]
                album_dir = os.path.dirname(file_paths[0]) if file_paths else None
                # Delete from beets DB
                conn.execute("DELETE FROM items WHERE album_id = ?", (int(album_id),))
                conn.execute("DELETE FROM albums WHERE id = ?", (int(album_id),))
                conn.commit()
                conn.close()
                # Delete files from disk
                deleted_files = 0
                if album_dir and os.path.isdir(album_dir):
                    shutil.rmtree(album_dir)
                    deleted_files = len(file_paths)
                self._json({
                    "status": "ok", "id": album_id,
                    "album": album_name, "artist": artist_name,
                    "deleted_files": deleted_files,
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
