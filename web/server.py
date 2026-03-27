#!/usr/bin/env python3
"""Soularr Web UI — album request manager at music.ablz.au.

Browse MusicBrainz, add releases to the pipeline DB, view status.

Usage:
    python3 web/server.py --port 8085 --dsn postgresql://soularr@192.168.100.11/soularr
"""

import argparse
import json
import logging
import os
import re
import shutil
import sqlite3
import sys
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("soularr-web")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
sys.path.insert(0, os.path.dirname(__file__))

import mb as mb_api
from pipeline_db import PipelineDB

_db_dsn = None


def _try_reconnect_db():
    """Reconnect the pipeline DB if the connection is dead."""
    global db
    if not _db_dsn:
        return
    try:
        db.conn.close()
    except Exception:
        pass
    try:
        db = PipelineDB(_db_dsn, run_migrations=False)
        log.info("Reconnected to pipeline DB")
    except Exception:
        log.exception("Failed to reconnect to pipeline DB")

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
    """Check beets library with track counts and audio quality. Returns dict of mbid → info."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return {}
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    placeholders = ",".join("?" for _ in mbids)
    rows = conn.execute(
        f"SELECT a.mb_albumid, "
        f"       (SELECT COUNT(*) FROM items WHERE items.album_id = a.id) as track_count, "
        f"       (SELECT GROUP_CONCAT(DISTINCT i.format) FROM items i WHERE i.album_id = a.id) as formats, "
        f"       (SELECT CAST(MIN(i.bitrate) AS INTEGER) FROM items i WHERE i.album_id = a.id) as min_bitrate, "
        f"       (SELECT MAX(i.samplerate) FROM items i WHERE i.album_id = a.id) as max_samplerate, "
        f"       (SELECT MAX(i.bitdepth) FROM items i WHERE i.album_id = a.id) as max_bitdepth "
        f"FROM albums a WHERE a.mb_albumid IN ({placeholders})", mbids
    ).fetchall()
    conn.close()
    return {r[0]: {
        "beets_tracks": r[1], "beets_format": r[2],
        "beets_bitrate": r[3], "beets_samplerate": r[4],
        "beets_bitdepth": r[5],
    } for r in rows}


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


def get_library_artist(artist_name, mb_artist_id=None):
    """Get albums by an artist from the beets library."""
    if not beets_db_path or not os.path.exists(beets_db_path):
        return []
    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
    # Match by MB artist ID (exact) plus name match for Discogs-only albums
    # Discogs IDs are numeric; MB UUIDs contain hyphens — use that to detect non-MB entries
    if mb_artist_id:
        rows = conn.execute(
            "SELECT album, albumartist, year, mb_albumid, discogs_albumid, "
            "       (SELECT COUNT(*) FROM items WHERE items.album_id = albums.id) as track_count "
            "FROM albums WHERE mb_albumartistid = ? OR mb_albumartistids LIKE ? "
            "  OR (albumartist LIKE ? COLLATE NOCASE "
            "      AND (mb_albumartistid IS NULL OR mb_albumartistid = '' "
            "           OR mb_albumartistid NOT LIKE '%-%')) "
            "ORDER BY year, album",
            (mb_artist_id, f"%{mb_artist_id}%", f"%{artist_name}%"),
        ).fetchall()
    else:
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
    """Check which MBIDs are already in the pipeline DB. Returns dict of mbid → info."""
    if not mbids or not db:
        return {}
    placeholders = ",".join(["%s"] * len(mbids))
    cur = db._execute(
        f"SELECT mb_release_id, status, quality_override "
        f"FROM album_requests WHERE mb_release_id IN ({placeholders})",
        tuple(mbids),
    )
    return {
        r["mb_release_id"]: {
            "status": r["status"],
            "quality_override": r["quality_override"],
        }
        for r in cur.fetchall()
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.info(fmt % args)

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
                mbid = params.get("mbid", [""])[0].strip()
                if not name:
                    self._error("Missing parameter 'name'")
                    return
                albums = get_library_artist(name, mbid)
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
                    pi = in_pipeline.get(r["id"])
                    r["pipeline_status"] = pi["status"] if pi else None
                self._json(data)

            elif re.match(r"^/api/release/[a-f0-9-]+$", path):
                release_id = path.split("/")[-1]
                data = mb_api.get_release(release_id)
                data["in_library"] = bool(check_beets_library([release_id]))
                req = db.get_request_by_mb_release_id(release_id)
                data["pipeline_status"] = req["status"] if req else None
                data["pipeline_id"] = req["id"] if req else None
                # Include beets track info if in library
                if data["in_library"] and beets_db_path and os.path.exists(beets_db_path):
                    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                    album = conn.execute(
                        "SELECT id FROM albums WHERE mb_albumid = ?", (release_id,)
                    ).fetchone()
                    if album:
                        items = conn.execute(
                            "SELECT title, track, disc, length, format, bitrate, "
                            "       samplerate, bitdepth "
                            "FROM items WHERE album_id = ? ORDER BY disc, track",
                            (album[0],),
                        ).fetchall()
                        data["beets_tracks"] = [{
                            "title": i[0], "track": i[1], "disc": i[2],
                            "length": i[3], "format": i[4], "bitrate": i[5],
                            "samplerate": i[6], "bitdepth": i[7],
                        } for i in items]
                    conn.close()
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
                        bi = beets_info[mbid]
                        item["beets_tracks"] = bi["beets_tracks"]
                        for k in ("beets_format", "beets_bitrate", "beets_samplerate", "beets_bitdepth"):
                            if bi.get(k):
                                item[k] = bi[k]
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
                    # Include latest download info (bitrate, user, etc.)
                    history = db.get_download_history(r["id"])
                    success = next((h for h in history if h.get("outcome") == "success"), None)
                    if success:
                        for k in ("soulseek_username", "filetype", "bitrate",
                                  "sample_rate", "bit_depth", "is_vbr",
                                  "was_converted", "original_filetype"):
                            val = success.get(k)
                            if val is not None:
                                item["dl_" + k] = val
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
                result = {
                    "request": {k: str(v) if hasattr(v, 'isoformat') else v for k, v in req.items()},
                    "tracks": tracks,
                    "history": [{k: str(v) if hasattr(v, 'isoformat') else v for k, v in h.items()} for h in history],
                }
                # Include beets tracks (with bitrate) if imported
                mbid = req.get("mb_release_id")
                if mbid and beets_db_path and os.path.exists(beets_db_path):
                    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                    album = conn.execute(
                        "SELECT id FROM albums WHERE mb_albumid = ?", (mbid,)
                    ).fetchone()
                    if album:
                        items = conn.execute(
                            "SELECT title, track, disc, length, format, "
                            "       bitrate, samplerate, bitdepth "
                            "FROM items WHERE album_id = ? ORDER BY disc, track",
                            (album[0],),
                        ).fetchall()
                        result["beets_tracks"] = [{
                            "title": i[0], "track": i[1], "disc": i[2],
                            "length": i[3], "format": i[4], "bitrate": i[5],
                            "samplerate": i[6], "bitdepth": i[7],
                        } for i in items]
                    conn.close()
                self._json(result)

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
                    "       a.added, a.mb_releasegroupid, a.release_group_title, "
                    "       (SELECT MIN(i.bitrate) FROM items i WHERE i.album_id = a.id) as min_bitrate "
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
                    "mb_releasegroupid": r[11], "release_group_title": r[12],
                    "min_bitrate": r[13],
                } for r in rows]
                # Add pipeline status for upgrade queue awareness
                if db:
                    mbids = [a["mb_albumid"] for a in albums if a.get("mb_albumid")]
                    pipeline_info = check_pipeline(mbids) if mbids else {}
                    for a in albums:
                        pi = pipeline_info.get(a.get("mb_albumid"))
                        if pi and pi["status"] == "wanted" and pi.get("quality_override"):
                            a["upgrade_queued"] = True
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
                result = {
                    "id": album[0], "album": album[1], "artist": album[2],
                    "year": album[3], "mb_albumid": album[4], "type": album[5],
                    "label": album[6], "country": album[7],
                    "artpath": album[8].decode("utf-8", errors="replace") if isinstance(album[8], bytes) else album[8],
                    "added": album[9], "tracks": tracks, "path": album_path,
                }
                # Include pipeline download history if available
                mb_id = album[4]
                if mb_id and db:
                    req = db.get_request_by_mb_release_id(mb_id)
                    if req:
                        history = db.get_download_history(req["id"])
                        result["pipeline_id"] = req["id"]
                        result["pipeline_status"] = req["status"]
                        result["pipeline_source"] = req["source"]
                        result["download_history"] = [
                            {k: str(v) if hasattr(v, 'isoformat') else v for k, v in h.items()}
                            for h in history
                        ]
                self._json(result)

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
                    "       a.added, a.mb_releasegroupid, a.release_group_title, "
                    "       (SELECT MIN(i.bitrate) FROM items i WHERE i.album_id = a.id) as min_bitrate "
                    "FROM albums a ORDER BY a.added DESC LIMIT 50",
                ).fetchall()
                conn.close()
                albums = [{
                    "id": r[0], "album": r[1], "artist": r[2], "year": r[3],
                    "mb_albumid": r[4], "type": r[5], "country": r[6],
                    "track_count": r[7], "formats": r[8], "added": r[9],
                    "mb_releasegroupid": r[10], "release_group_title": r[11],
                    "min_bitrate": r[12],
                } for r in rows]
                if db:
                    mbids = [a["mb_albumid"] for a in albums if a.get("mb_albumid")]
                    pipeline_info = check_pipeline(mbids) if mbids else {}
                    for a in albums:
                        pi = pipeline_info.get(a.get("mb_albumid"))
                        if pi and pi["status"] == "wanted" and pi.get("quality_override"):
                            a["upgrade_queued"] = True
                self._json({"albums": albums})

            else:
                self._error("Not found", 404)

        except Exception as e:
            log.exception("GET %s failed", path)
            _try_reconnect_db()
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

            elif path == "/api/pipeline/upgrade":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                mbid = body.get("mb_release_id", "").strip()
                if not mbid:
                    self._error("Missing mb_release_id")
                    return

                quality = "flac,mp3 v0,mp3 320"

                # Calculate min_bitrate from beets library
                min_bitrate = None
                if beets_db_path and os.path.exists(beets_db_path):
                    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                    album_row = conn.execute(
                        "SELECT id FROM albums WHERE mb_albumid = ?", (mbid,)
                    ).fetchone()
                    if album_row:
                        br_row = conn.execute(
                            "SELECT MIN(bitrate) FROM items WHERE album_id = ?",
                            (album_row[0],),
                        ).fetchone()
                        if br_row and br_row[0]:
                            min_bitrate = int(br_row[0] / 1000)
                    conn.close()

                # Find or create pipeline request
                existing = db.get_request_by_mb_release_id(mbid)
                if existing:
                    req_id = existing["id"]
                    db.reset_to_wanted(req_id,
                                       quality_override=quality,
                                       min_bitrate=min_bitrate)
                    self._json({
                        "status": "upgrade_queued",
                        "id": req_id,
                        "min_bitrate": min_bitrate,
                        "quality_override": quality,
                    })
                else:
                    # Album in beets but not in pipeline DB — create new request.
                    # Try MB API first, fall back to beets DB for non-UUID IDs (e.g. Discogs).
                    release = None
                    try:
                        release = mb_api.get_release(mbid)
                    except Exception:
                        pass
                    if release:
                        artist_name = release["artist_name"]
                        album_title = release["title"]
                        year = release.get("year")
                        country = release.get("country")
                        artist_id = release.get("artist_id")
                        tracks = release.get("tracks")
                    else:
                        # Fall back to beets DB
                        beets_info = None
                        if beets_db_path and os.path.exists(beets_db_path):
                            bconn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                            beets_info = bconn.execute(
                                "SELECT albumartist, album, year FROM albums WHERE mb_albumid = ?",
                                (mbid,),
                            ).fetchone()
                            bconn.close()
                        if not beets_info:
                            self._error("Release not found in MusicBrainz or beets")
                            return
                        artist_name = beets_info[0]
                        album_title = beets_info[1]
                        year = str(beets_info[2]) if beets_info[2] else None
                        country = None
                        artist_id = None
                        tracks = None
                    req_id = db.add_request(
                        mb_release_id=mbid,
                        mb_artist_id=artist_id,
                        artist_name=artist_name,
                        album_title=album_title,
                        year=year,
                        country=country,
                        source="request",
                    )
                    if tracks:
                        db.set_tracks(req_id, tracks)
                    db.reset_to_wanted(req_id,
                                       quality_override=quality,
                                       min_bitrate=min_bitrate)
                    self._json({
                        "status": "upgrade_queued",
                        "id": req_id,
                        "min_bitrate": min_bitrate,
                        "quality_override": quality,
                        "created": True,
                    })

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
            log.exception("POST %s failed", path)
            _try_reconnect_db()
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

    global _db_dsn
    _db_dsn = args.dsn
    db = PipelineDB(args.dsn, run_migrations=False)
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
