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
    if db is not None:
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
db: PipelineDB | None = None
beets_db_path: str | None = None


def _db() -> PipelineDB:
    """Return the pipeline DB, raising if not connected."""
    if db is None:
        raise RuntimeError("Pipeline DB not connected")
    return db


def _serialize_row(row: dict[str, object]) -> dict[str, object]:
    """Serialize a DB row dict — convert datetime objects to ISO strings."""
    result: dict[str, object] = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()  # type: ignore[union-attr]
        else:
            result[k] = v
    return result


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
    pdb = _db()
    placeholders = ",".join(["%s"] * len(mbids))
    cur = pdb._execute(
        f"SELECT mb_release_id, status, quality_override, min_bitrate "
        f"FROM album_requests WHERE mb_release_id IN ({placeholders})",
        tuple(mbids),
    )
    return {
        r["mb_release_id"]: {
            "status": r["status"],
            "quality_override": r["quality_override"],
            "min_bitrate": r["min_bitrate"],
        }
        for r in cur.fetchall()
    }


class Handler(BaseHTTPRequestHandler):

    _GET_ROUTES: dict[str, str] = {
        "/": "_get_index",
        "/api/search": "_get_search",
        "/api/library/artist": "_get_library_artist",
        "/api/pipeline/log": "_get_pipeline_log",
        "/api/pipeline/status": "_get_pipeline_status",
        "/api/pipeline/recent": "_get_pipeline_recent",
        "/api/pipeline/all": "_get_pipeline_all",
        "/api/pipeline/constants": "_get_pipeline_constants",
        "/api/pipeline/simulate": "_get_pipeline_simulate",
        "/api/beets/search": "_get_beets_search",
        "/api/beets/recent": "_get_beets_recent",
    }

    _GET_PATTERNS: list[tuple[re.Pattern[str], str]] = [
        (re.compile(r"^/api/artist/([a-f0-9-]+)$"), "_get_artist"),
        (re.compile(r"^/api/release-group/([a-f0-9-]+)$"), "_get_release_group"),
        (re.compile(r"^/api/release/([a-f0-9-]+)$"), "_get_release"),
        (re.compile(r"^/api/pipeline/(\d+)$"), "_get_pipeline_detail"),
        (re.compile(r"^/api/beets/album/(\d+)$"), "_get_beets_album"),
    ]

    _POST_ROUTES: dict[str, str] = {
        "/api/pipeline/add": "_post_pipeline_add",
        "/api/pipeline/update": "_post_pipeline_update",
        "/api/pipeline/upgrade": "_post_pipeline_upgrade",
        "/api/pipeline/set-quality": "_post_pipeline_set_quality",
        "/api/pipeline/ban-source": "_post_pipeline_ban_source",
        "/api/pipeline/force-import": "_post_pipeline_force_import",
        "/api/pipeline/delete": "_post_pipeline_delete",
        "/api/beets/delete": "_post_beets_delete",
    }

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        log.info(format % args)

    def _json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def _html(self, path):
        html_path = os.path.join(os.path.dirname(__file__), path)
        with open(html_path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _error(self, msg, status=400):
        self._json({"error": msg}, status)

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

        try:
            method_name = self._GET_ROUTES.get(path)
            if method_name:
                getattr(self, method_name)(params)
                return
            for pattern, method_name in self._GET_PATTERNS:
                m = pattern.match(path)
                if m:
                    getattr(self, method_name)(params, *m.groups())
                    return
            self._error("Not found", 404)
        except Exception as e:
            log.exception("GET %s failed", path)
            _try_reconnect_db()
            self._error(str(e), 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            method_name = self._POST_ROUTES.get(path)
            if method_name:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                getattr(self, method_name)(body)
                return
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

    # ── GET handlers ─────────────────────────────────────────────────

    def _get_index(self, params: dict[str, list[str]]) -> None:
        self._html("index.html")

    def _get_search(self, params: dict[str, list[str]]) -> None:
        q = params.get("q", [""])[0].strip()
        if not q:
            self._error("Missing query parameter 'q'")
            return
        artists = mb_api.search_artists(q)
        self._json({"artists": artists})

    def _get_library_artist(self, params: dict[str, list[str]]) -> None:
        name = params.get("name", [""])[0].strip()
        mbid = params.get("mbid", [""])[0].strip()
        if not name:
            self._error("Missing parameter 'name'")
            return
        albums = get_library_artist(name, mbid)
        self._json({"albums": albums})

    def _get_artist(self, params: dict[str, list[str]], artist_id: str) -> None:
        rgs = mb_api.get_artist_release_groups(artist_id)
        official_rg_ids = mb_api.get_official_release_group_ids(artist_id)
        for rg in rgs:
            rg["has_official"] = rg["id"] in official_rg_ids
        self._json({"release_groups": rgs})

    def _get_release_group(self, params: dict[str, list[str]], rg_id: str) -> None:
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

    def _get_release(self, params: dict[str, list[str]], release_id: str) -> None:
        data = mb_api.get_release(release_id)
        data["in_library"] = bool(check_beets_library([release_id]))
        req = _db().get_request_by_mb_release_id(release_id)
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

    def _get_pipeline_log(self, params: dict[str, list[str]]) -> None:
        from classify import classify_log_entry, LogEntry
        outcome_filter = params.get("outcome", [None])[0]
        if outcome_filter not in (None, "imported", "rejected"):
            outcome_filter = None
        entries = _db().get_log(limit=50, outcome_filter=outcome_filter)
        mbids = list(set(e["mb_release_id"] for e in entries if e.get("mb_release_id")))
        beets_info = check_beets_library_detail(mbids) if mbids else {}
        result = []
        for e in entries:
            entry = LogEntry.from_row(e)
            classified = classify_log_entry(entry)
            item = entry.to_json_dict()
            mbid = entry.mb_release_id
            bi = beets_info.get(mbid) if mbid else None
            item["in_beets"] = bi is not None
            if bi:
                item["beets_format"] = bi.get("beets_format")
                item["beets_bitrate"] = bi.get("beets_bitrate")
            item["badge"] = classified.badge
            item["badge_class"] = classified.badge_class
            item["border_color"] = classified.border_color
            item["verdict"] = classified.verdict
            item["summary"] = classified.summary
            result.append(item)
        # Count outcomes for filter buttons (single query, no limit)
        count_cur = _db()._execute("""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE outcome IN ('success', 'force_import')) AS imported
            FROM download_log
        """)
        count_row = count_cur.fetchone()
        total = count_row["total"] if count_row else 0
        imported_c = count_row["imported"] if count_row else 0
        self._json({
            "log": result,
            "counts": {
                "all": total,
                "imported": imported_c,
                "rejected": total - imported_c,
            },
        })

    def _get_pipeline_status(self, params: dict[str, list[str]]) -> None:
        counts = _db().count_by_status()
        wanted = _db().get_wanted(limit=50)
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

    def _get_pipeline_recent(self, params: dict[str, list[str]]) -> None:
        recent = _db().get_recent(limit=20)
        # Check beets library for each and get track counts
        mbids = [r["mb_release_id"] for r in recent if r.get("mb_release_id")]
        beets_info = check_beets_library_detail(mbids) if mbids else {}
        serialized = []
        for r in recent:
            item = _serialize_row(r)
            mbid = r.get("mb_release_id")
            pipeline_tracks = len(_db().get_tracks(r["id"]))
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
            history = _db().get_download_history(r["id"])
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

    def _get_pipeline_all(self, params: dict[str, list[str]]) -> None:
        from classify import classify_log_entry, LogEntry
        counts = _db().count_by_status()
        all_data: dict[str, object] = {"counts": counts}
        for status in ("wanted", "imported", "manual"):
            items = []
            for r in _db().get_by_status(status):
                item = _serialize_row(r)
                # Add last download verdict for context
                history = _db().get_download_history(r["id"])
                if history:
                    last = history[0]
                    entry = LogEntry.from_row(last)
                    classified = classify_log_entry(entry)
                    item["last_verdict"] = classified.verdict
                    item["last_outcome"] = last.get("outcome")
                    item["last_username"] = last.get("soulseek_username")
                    item["download_count"] = len(history)
                items.append(item)
            all_data[status] = items
        self._json(all_data)

    def _get_pipeline_constants(self, params: dict[str, list[str]]) -> None:
        """Return decision tree structure + thresholds for the diagram."""
        from quality import get_decision_tree
        from spectral_check import (HF_DEFICIT_SUSPECT, HF_DEFICIT_MARGINAL,
                                    ALBUM_SUSPECT_PCT, MIN_CLIFF_SLICES,
                                    CLIFF_THRESHOLD_DB_PER_KHZ)
        tree = get_decision_tree()
        tree["constants"]["HF_DEFICIT_SUSPECT"] = HF_DEFICIT_SUSPECT
        tree["constants"]["HF_DEFICIT_MARGINAL"] = HF_DEFICIT_MARGINAL
        tree["constants"]["ALBUM_SUSPECT_PCT"] = ALBUM_SUSPECT_PCT
        tree["constants"]["MIN_CLIFF_SLICES"] = MIN_CLIFF_SLICES
        tree["constants"]["CLIFF_THRESHOLD_DB_PER_KHZ"] = CLIFF_THRESHOLD_DB_PER_KHZ
        self._json(tree)

    def _get_pipeline_simulate(self, params: dict[str, list[str]]) -> None:
        """Run full_pipeline_decision() with query-string inputs."""
        from quality import full_pipeline_decision

        def _str(key: str) -> str | None:
            v = params.get(key, [None])[0]
            return v if v else None

        def _int(key: str) -> int | None:
            v = _str(key)
            return int(v) if v else None

        def _bool(key: str) -> bool:
            v = _str(key)
            return v in ("true", "1", "yes") if v else False

        result = full_pipeline_decision(
            is_flac=_bool("is_flac"),
            min_bitrate=_int("min_bitrate") or 0,
            is_cbr=_bool("is_cbr"),
            spectral_grade=_str("spectral_grade"),
            spectral_bitrate=_int("spectral_bitrate"),
            existing_min_bitrate=_int("existing_min_bitrate"),
            existing_spectral_bitrate=_int("existing_spectral_bitrate"),
            override_min_bitrate=_int("override_min_bitrate"),
            post_conversion_min_bitrate=_int("post_conversion_min_bitrate"),
            converted_count=_int("converted_count") or 0,
            verified_lossless=_bool("verified_lossless"),
        )
        self._json(result)

    def _get_pipeline_detail(self, params: dict[str, list[str]], req_id_str: str) -> None:
        req_id = int(req_id_str)
        req = _db().get_request(req_id)
        if not req:
            self._error("Not found", 404)
            return
        tracks = _db().get_tracks(req_id)
        history = _db().get_download_history(req_id)
        from classify import classify_log_entry, LogEntry
        history_items = []
        for h in history:
            he = LogEntry.from_row(h)
            hi = he.to_json_dict()
            classified = classify_log_entry(he)
            hi["verdict"] = classified.verdict
            hi["downloaded_label"] = classified.downloaded_label
            history_items.append(hi)
        result: dict[str, object] = {
            "request": _serialize_row(req),
            "tracks": tracks,
            "history": history_items,
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

    def _get_beets_search(self, params: dict[str, list[str]]) -> None:
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
                if pi:
                    if pi["status"] == "wanted" and pi.get("quality_override"):
                        a["upgrade_queued"] = True
                    # Use pipeline min_bitrate (avg after accept) if higher than beets min
                    if pi.get("min_bitrate") and a.get("min_bitrate"):
                        pi_br = pi["min_bitrate"] * 1000  # DB stores kbps, beets stores bps
                        if pi_br > a["min_bitrate"]:
                            a["min_bitrate"] = pi_br
        self._json({"albums": albums})

    def _get_beets_album(self, params: dict[str, list[str]], album_id_str: str) -> None:
        album_id = int(album_id_str)
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
        result: dict[str, object] = {
            "id": album[0], "album": album[1], "artist": album[2],
            "year": album[3], "mb_albumid": album[4], "type": album[5],
            "label": album[6], "country": album[7],
            "artpath": album[8].decode("utf-8", errors="replace") if isinstance(album[8], bytes) else album[8],
            "added": album[9], "tracks": tracks, "path": album_path,
        }
        # Include pipeline download history if available
        mb_id = album[4]
        if mb_id and db:
            req = _db().get_request_by_mb_release_id(mb_id)
            if req:
                history = _db().get_download_history(req["id"])
                result["pipeline_id"] = req["id"]
                result["pipeline_status"] = req["status"]
                result["pipeline_source"] = req["source"]
                result["pipeline_min_bitrate"] = req.get("min_bitrate")
                result["upgrade_queued"] = (
                    req["status"] == "wanted" and bool(req.get("quality_override"))
                )
                from classify import classify_log_entry as _clf, LogEntry as _LE
                dh = []
                for h in history:
                    he = _LE.from_row(h)
                    hi = he.to_json_dict()
                    _c = _clf(he)
                    hi["verdict"] = _c.verdict
                    hi["downloaded_label"] = _c.downloaded_label
                    dh.append(hi)
                result["download_history"] = dh
        self._json(result)

    def _get_beets_recent(self, params: dict[str, list[str]]) -> None:
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
                if pi:
                    if pi["status"] == "wanted" and pi.get("quality_override"):
                        a["upgrade_queued"] = True
                    if pi.get("min_bitrate") and a.get("min_bitrate"):
                        pi_br = pi["min_bitrate"] * 1000
                        if pi_br > a["min_bitrate"]:
                            a["min_bitrate"] = pi_br
        self._json({"albums": albums})

    # ── POST handlers ────────────────────────────────────────────────

    def _post_pipeline_add(self, body: dict) -> None:
        mbid = body.get("mb_release_id", "").strip()
        source = body.get("source", "request")

        if not mbid:
            self._error("Missing mb_release_id")
            return

        # Check if already exists
        existing = _db().get_request_by_mb_release_id(mbid)
        if existing:
            self._json({
                "status": "exists",
                "id": existing["id"],
                "current_status": existing["status"],
            })
            return

        # Fetch from MB
        release = mb_api.get_release(mbid)

        req_id = _db().add_request(
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
            _db().set_tracks(req_id, release["tracks"])

        self._json({
            "status": "added",
            "id": req_id,
            "artist": release["artist_name"],
            "album": release["title"],
            "tracks": len(release.get("tracks", [])),
        })

    def _post_pipeline_update(self, body: dict) -> None:
        req_id = body.get("id")
        new_status = body.get("status", "").strip()

        if not req_id or not new_status:
            self._error("Missing id or status")
            return
        if new_status not in ("wanted", "imported", "manual"):
            self._error(f"Invalid status: {new_status}")
            return

        req = _db().get_request(int(req_id))
        if not req:
            self._error("Not found", 404)
            return

        if new_status == "wanted" and req["status"] != "wanted":
            # If album is in beets, set quality override for upgrade search
            mbid = req.get("mb_release_id")
            quality = None
            min_br = None
            if mbid and beets_db_path and os.path.exists(beets_db_path):
                conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                album_row = conn.execute(
                    "SELECT id FROM albums WHERE mb_albumid = ?", (mbid,)
                ).fetchone()
                if album_row:
                    quality = "flac,mp3 v0,mp3 320"
                    br_row = conn.execute(
                        "SELECT CAST(MIN(bitrate) AS INTEGER) FROM items WHERE album_id = ?",
                        (album_row[0],),
                    ).fetchone()
                    if br_row and br_row[0]:
                        min_br = int(br_row[0] / 1000)
                conn.close()
            _db().reset_to_wanted(int(req_id), quality_override=quality, min_bitrate=min_br)
        else:
            _db().update_status(int(req_id), new_status)

        self._json({"status": "ok", "id": req_id, "new_status": new_status})

    def _post_pipeline_upgrade(self, body: dict) -> None:
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
        existing = _db().get_request_by_mb_release_id(mbid)
        if existing:
            req_id = existing["id"]
            _db().reset_to_wanted(req_id,
                               quality_override=quality,
                               min_bitrate=min_bitrate)
            self._json({
                "status": "upgrade_queued",
                "id": req_id,
                "min_bitrate": min_bitrate,
                "quality_override": quality,
            })
        else:
            # Album in beets but not in pipeline DB — create new request
            release = mb_api.get_release(mbid)
            req_id = _db().add_request(
                mb_release_id=mbid,
                mb_artist_id=release.get("artist_id"),
                artist_name=release["artist_name"],
                album_title=release["title"],
                year=release.get("year"),
                country=release.get("country"),
                source="request",
            )
            if release.get("tracks"):
                _db().set_tracks(req_id, release["tracks"])
            _db().reset_to_wanted(req_id,
                               quality_override=quality,
                               min_bitrate=min_bitrate)
            self._json({
                "status": "upgrade_queued",
                "id": req_id,
                "min_bitrate": min_bitrate,
                "quality_override": quality,
                "created": True,
            })

    def _post_pipeline_set_quality(self, body: dict) -> None:
        mbid = body.get("mb_release_id", "").strip()
        new_status = body.get("status", "").strip()
        min_bitrate = body.get("min_bitrate")

        if not mbid:
            self._error("Missing mb_release_id")
            return

        existing = _db().get_request_by_mb_release_id(mbid)
        if not existing:
            self._error("Not found in pipeline", 404)
            return

        req_id = existing["id"]

        # Set min_bitrate override
        if min_bitrate is not None:
            min_bitrate = int(min_bitrate)
            _db()._execute(
                "UPDATE album_requests SET min_bitrate = %s WHERE id = %s",
                (min_bitrate, req_id),
            )

        # Set status
        if new_status:
            if new_status not in ("wanted", "imported", "manual"):
                self._error(f"Invalid status: {new_status}")
                return
            if new_status == "imported":
                # Auto-set min_bitrate to average if not explicitly provided
                if min_bitrate is None and mbid and beets_db_path and os.path.exists(beets_db_path):
                    conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
                    album_row = conn.execute(
                        "SELECT id FROM albums WHERE mb_albumid = ?", (mbid,)
                    ).fetchone()
                    if album_row:
                        avg_row = conn.execute(
                            "SELECT CAST(AVG(bitrate) AS INTEGER) FROM items WHERE album_id = ?",
                            (album_row[0],),
                        ).fetchone()
                        if avg_row and avg_row[0]:
                            min_bitrate = int(avg_row[0] / 1000)
                    conn.close()
                # Clear quality_override so it stops looping
                sets = "status = 'imported', quality_override = NULL, updated_at = NOW()"
                if min_bitrate is not None:
                    sets += f", min_bitrate = {int(min_bitrate)}"
                _db()._execute(
                    f"UPDATE album_requests SET {sets} WHERE id = %s",
                    (req_id,),
                )
            elif new_status == "wanted" and existing["status"] != "wanted":
                _db().reset_to_wanted(req_id)
            else:
                _db().update_status(req_id, new_status)

        self._json({
            "status": "ok",
            "id": req_id,
            "new_status": new_status or existing["status"],
            "min_bitrate": min_bitrate,
        })

    def _post_pipeline_ban_source(self, body: dict) -> None:
        req_id = body.get("request_id")
        username = body.get("username", "").strip()
        mb_release_id = body.get("mb_release_id", "").strip()

        if not req_id or not username:
            self._error("Missing request_id or username")
            return

        # 1. Denylist the user for this album
        _db().add_denylist(int(req_id), username, "manually banned via web UI")

        # 2. Remove from beets if present
        beets_removed = False
        if mb_release_id and beets_db_path and os.path.exists(beets_db_path):
            conn = sqlite3.connect(f"file:{beets_db_path}?mode=ro", uri=True)
            album_row = conn.execute(
                "SELECT id FROM albums WHERE mb_albumid = ?", (mb_release_id,)
            ).fetchone()
            conn.close()
            if album_row:
                # Use beet remove (without -d to keep files? No, remove completely)
                import subprocess as _sp
                result = _sp.run(
                    ["beet", "remove", "-d", f"mb_albumid:{mb_release_id}"],
                    capture_output=True, text=True, timeout=30,
                    env={**os.environ, "HOME": "/home/abl030"},
                )
                beets_removed = result.returncode == 0

        # 3. Requeue for re-download
        req = _db().get_request(int(req_id))
        if req:
            quality = req.get("quality_override") or "flac,mp3 v0,mp3 320"
            min_br = req.get("min_bitrate")
            _db().reset_to_wanted(int(req_id), quality_override=quality, min_bitrate=min_br)

        self._json({
            "status": "ok",
            "username": username,
            "beets_removed": beets_removed,
        })

    def _post_pipeline_force_import(self, body: dict) -> None:
        log_id = body.get("download_log_id")

        if not log_id:
            self._error("Missing download_log_id")
            return

        # Look up download_log entry
        entry = _db().get_download_log_entry(int(log_id))
        if not entry:
            self._error(f"Download log entry {log_id} not found", 404)
            return

        request_id = entry["request_id"]

        # Extract failed_path from validation_result
        vr_raw = entry.get("validation_result")
        if not vr_raw:
            self._error("No validation_result on this download log entry")
            return
        vr = vr_raw if isinstance(vr_raw, dict) else json.loads(vr_raw)
        failed_path = vr.get("failed_path")
        if not failed_path:
            self._error("No failed_path in validation_result")
            return

        # Look up album request for MBID
        req = _db().get_request(request_id)
        if not req:
            self._error(f"Album request {request_id} not found", 404)
            return
        mbid = req.get("mb_release_id")
        if not mbid:
            self._error("Album has no MusicBrainz release ID")
            return

        # Resolve failed_path (may be relative from old entries)
        if not os.path.isdir(failed_path):
            for base in ["/mnt/virtio/music/slskd"]:
                candidate = os.path.join(base, failed_path)
                if os.path.isdir(candidate):
                    failed_path = candidate
                    break
        if not os.path.isdir(failed_path):
            self._error(f"Files not found at: {failed_path}")
            return

        # Run import_one.py --force
        import_one_path = os.path.join(
            os.path.dirname(__file__), "..", "harness", "import_one.py")
        cmd = [
            sys.executable, import_one_path,
            failed_path, mbid,
            "--request-id", str(request_id),
            "--force",
        ]
        if req.get("min_bitrate"):
            cmd.extend(["--override-min-bitrate", str(req["min_bitrate"])])

        import subprocess as _sp
        result = _sp.run(cmd, capture_output=True, text=True, timeout=1800)

        # Parse ImportResult from stdout
        import_result_json = None
        for line in result.stdout.splitlines():
            if "__IMPORT_RESULT__" in line:
                try:
                    import_result_json = line.split("__IMPORT_RESULT__")[1].strip()
                except (IndexError, json.JSONDecodeError):
                    pass

        # Log to download_log
        _db().log_download(
            request_id=request_id,
            outcome="force_import",
            import_result=import_result_json,
            staged_path=failed_path,
        )

        if result.returncode == 0:
            update_fields: dict[str, object] = {}
            # Propagate spectral data from ImportResult to album_requests
            if import_result_json:
                try:
                    ir = json.loads(import_result_json)
                    spectral = ir.get("spectral", {})
                    if spectral.get("grade"):
                        update_fields["spectral_grade"] = spectral["grade"]
                    if spectral.get("bitrate") is not None:
                        update_fields["spectral_bitrate"] = spectral["bitrate"]
                    # Check verified lossless
                    conv = ir.get("conversion", {})
                    quality = ir.get("quality", {})
                    if (conv.get("was_converted") and
                            conv.get("original_filetype", "").lower() == "flac" and
                            spectral.get("grade") == "genuine"):
                        update_fields["verified_lossless"] = True
                    if quality.get("new_min_bitrate") is not None:
                        update_fields["min_bitrate"] = quality["new_min_bitrate"]
                except (json.JSONDecodeError, TypeError):
                    pass
            _db().update_status(request_id, "imported", **update_fields)

        self._json({
            "status": "ok" if result.returncode == 0 else "error",
            "exit_code": result.returncode,
            "request_id": request_id,
            "artist": req["artist_name"],
            "album": req["album_title"],
            "stderr": result.stderr[-500:] if result.stderr else "",
        })

    def _post_pipeline_delete(self, body: dict) -> None:
        req_id = body.get("id")
        if not req_id:
            self._error("Missing id")
            return
        req = _db().get_request(int(req_id))
        if not req:
            self._error("Not found", 404)
            return
        _db().delete_request(int(req_id))
        self._json({"status": "ok", "id": req_id})

    def _post_beets_delete(self, body: dict) -> None:
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
    _db().close()


if __name__ == "__main__":
    main()
