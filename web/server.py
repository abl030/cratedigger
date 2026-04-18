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

# Ensure this module is importable as 'web.server' even when run as __main__,
# so route modules can `from web import server` and get the same instance.
if __name__ == "__main__" or "web.server" not in sys.modules:
    sys.modules["web.server"] = sys.modules[__name__]

import web.cache as cache
import mb as mb_api
from beets_db import BeetsDB
from pipeline_db import PipelineDB
from routes import browse as _browse_routes  # type: ignore[import-untyped]
from routes import library as _library_routes  # type: ignore[import-untyped]
from routes import imports as _imports_routes  # type: ignore[import-untyped]
from routes import pipeline as _pipeline_routes  # type: ignore[import-untyped]

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
        db = PipelineDB(_db_dsn)
        log.info("Reconnected to pipeline DB")
    except Exception:
        log.exception("Failed to reconnect to pipeline DB")

# Globals set in main()
db: PipelineDB | None = None
beets_db_path: str | None = None
_beets: BeetsDB | None = None


def _db() -> PipelineDB:
    """Return the pipeline DB, raising if not connected."""
    if db is None:
        raise RuntimeError("Pipeline DB not connected")
    return db


def _beets_db() -> BeetsDB | None:
    """Return the BeetsDB instance, or None if not configured."""
    return _beets


def _serialize_row(row: dict[str, object]) -> dict[str, object]:
    """Serialize a DB row dict — convert datetime objects to ISO strings."""
    result: dict[str, object] = {}
    for k, v in row.items():
        if hasattr(v, "isoformat"):
            result[k] = v.isoformat()  # type: ignore[union-attr]
        else:
            result[k] = v
    return result


def check_beets_library(mbids: list[str] | list[object]) -> set[str]:
    """Check which MBIDs are already in the beets library."""
    b = _beets_db()
    return b.check_mbids([str(m) for m in mbids]) if b else set()


def check_beets_library_detail(mbids: list[str] | list[object]) -> dict[str, dict[str, object]]:
    """Check beets library with track counts and audio quality."""
    b = _beets_db()
    return b.check_mbids_detail([str(m) for m in mbids]) if b else {}


def check_beets_by_artist_album(artist: str, album: str) -> int | None:
    """Fuzzy check: is there an album by this artist in beets? Returns track count or None."""
    b = _beets_db()
    return b.find_by_artist_album(artist, album) if b else None


def get_library_artist(artist_name, mb_artist_id=None):
    """Get albums by an artist from the beets library."""
    b = _beets_db()
    if not b:
        return []
    return b.get_albums_by_artist(artist_name, mb_artist_id or "")


def check_pipeline(mbids):
    """Check which MBIDs are already in the pipeline DB. Returns dict of mbid → info."""
    if not mbids or not db:
        return {}
    pdb = _db()
    placeholders = ",".join(["%s"] * len(mbids))
    cur = pdb._execute(
        f"SELECT id, mb_release_id, status, search_filetype_override, target_format, min_bitrate "
        f"FROM album_requests WHERE mb_release_id IN ({placeholders})",
        tuple(mbids),
    )
    return {
        r["mb_release_id"]: {
            "id": r["id"],
            "status": r["status"],
            "search_filetype_override": r["search_filetype_override"],
            "target_format": r["target_format"],
            "min_bitrate": r["min_bitrate"],
        }
        for r in cur.fetchall()
    }


def _enrich_with_pipeline(albums: list[dict[str, object]]) -> None:
    """Add pipeline_status/upgrade_queued to album dicts. Mutates in place."""
    if not db:
        return
    mbids = [str(a["mb_albumid"]) for a in albums if a.get("mb_albumid")]
    if not mbids:
        return
    pipeline_info = check_pipeline(mbids)
    for a in albums:
        pi = pipeline_info.get(a.get("mb_albumid"))
        if pi:
            apply_pipeline_bitrate_override(a, pi)


def apply_pipeline_bitrate_override(album: dict, pipeline_info: dict) -> None:
    """Apply pipeline DB min_bitrate and upgrade_queued flag to a beets album dict.

    Pipeline DB stores kbps, beets stores bps. Only overrides when pipeline is higher.
    """
    if pipeline_info.get("status") == "wanted" and (pipeline_info.get("search_filetype_override") or pipeline_info.get("target_format")):
        album["upgrade_queued"] = True
    pi_br = pipeline_info.get("min_bitrate")
    a_br = album.get("min_bitrate")
    if pi_br is not None and a_br is not None:
        pi_br_bps = pi_br * 1000  # kbps → bps
        if pi_br_bps > a_br:
            album["min_bitrate"] = pi_br_bps


class Handler(BaseHTTPRequestHandler):

    # Route tables: path → handler function.
    # Route modules export their own dicts; we merge them here.
    _FUNC_GET_ROUTES: dict[str, object] = {
        **_browse_routes.GET_ROUTES,
        **_pipeline_routes.GET_ROUTES,
        **_library_routes.GET_ROUTES,
        **_imports_routes.GET_ROUTES,
    }

    _FUNC_GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
        *_browse_routes.GET_PATTERNS,
        *_pipeline_routes.GET_PATTERNS,
        *_library_routes.GET_PATTERNS,
    ]

    _FUNC_POST_ROUTES: dict[str, object] = {
        **_pipeline_routes.POST_ROUTES,
        **_library_routes.POST_ROUTES,
        **_imports_routes.POST_ROUTES,
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
        # Capture for routing-level cache (set by do_GET)
        key = getattr(self, "_cache_capture_key", None)
        ttl = getattr(self, "_cache_capture_ttl", None)
        if status == 200 and key is not None and ttl is not None:
            cache.cache_set(key, data, ttl)

    def _html(self, path):
        html_path = os.path.join(os.path.dirname(__file__), path)
        with open(html_path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _static_js(self, path):
        """Serve a JS file from the web/js/ directory."""
        js_path = os.path.join(os.path.dirname(__file__), "js", os.path.basename(path))
        if not os.path.isfile(js_path):
            self._error("Not found", 404)
            return
        with open(js_path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", "application/javascript; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, msg, status=400):
        self._json({"error": msg}, status)

    # Routes that should be cached, mapped to their TTL.
    # Prefix-matched: "/api/artist" matches "/api/artist/<id>" etc.
    _CACHE_TTLS: dict[str, int] = {
        "/api/search": cache.TTL_MB,
        "/api/artist": cache.TTL_MB,
        "/api/release-group": cache.TTL_MB,
        "/api/release": cache.TTL_MB,
        "/api/discogs": cache.TTL_MB,
        "/api/library": cache.TTL_LIBRARY,
        "/api/beets": cache.TTL_LIBRARY,
        "/api/pipeline/status": cache.TTL_LIBRARY,
        "/api/pipeline/all": cache.TTL_LIBRARY,
        "/api/pipeline/recent": cache.TTL_LIBRARY,
        "/api/pipeline/log": cache.TTL_LIBRARY,
    }

    # POST routes and which cache groups they invalidate.
    _POST_INVALIDATIONS: dict[str, tuple[str, ...]] = {
        "/api/pipeline/add": ("pipeline", "mb", "discogs"),
        "/api/pipeline/update": ("pipeline", "mb", "discogs"),
        "/api/pipeline/upgrade": ("pipeline", "library", "mb", "discogs"),
        "/api/pipeline/set-quality": ("pipeline",),
        "/api/pipeline/set-intent": ("pipeline",),
        "/api/pipeline/ban-source": ("pipeline", "library", "mb", "discogs"),
        "/api/pipeline/force-import": ("pipeline", "library", "mb", "discogs"),
        "/api/pipeline/delete": ("pipeline", "mb", "discogs"),
        "/api/beets/delete": ("library", "mb", "discogs"),
        "/api/manual-import/import": ("pipeline", "library", "mb", "discogs"),
        "/api/wrong-matches/delete": ("pipeline",),
    }

    def _cache_ttl_for_path(self, path: str) -> int | None:
        """Return TTL if this path should be cached, None otherwise."""
        for prefix, ttl in self._CACHE_TTLS.items():
            if path == prefix or path.startswith(prefix + "/"):
                return ttl
        return None

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

        # Cache check: use full URL (path + query) as key
        cache_key = f"web:{path}"
        if parsed.query:
            cache_key += f"?{parsed.query}"
        ttl = self._cache_ttl_for_path(path)
        if ttl is not None:
            cached = cache.cache_get(cache_key)
            if cached is not None:
                self._json(cached)
                return
            # Set up capture so _json() stores the response
            self._cache_capture_key = cache_key
            self._cache_capture_ttl = ttl

        try:
            # Serve static JS modules
            if path.startswith("/js/") and path.endswith(".js"):
                self._static_js(path[4:])
                return

            # Check local method (index)
            if path == "/":
                self._get_index(params)
                return

            # Check route module handlers
            fn = self._FUNC_GET_ROUTES.get(path)
            if fn:
                fn(self, params)  # type: ignore[operator]
                return
            for pattern, fn in self._FUNC_GET_PATTERNS:
                m = pattern.match(path)
                if m:
                    fn(self, params, *m.groups())  # type: ignore[operator]
                    return
            self._error("Not found", 404)
        except Exception as e:
            log.exception("GET %s failed", path)
            _try_reconnect_db()
            self._error(str(e), 500)
        finally:
            self._cache_capture_key = None
            self._cache_capture_ttl = None

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            # Cache invalidation endpoint (for soularr main loop)
            if path == "/api/cache/invalidate":
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                groups = body.get("groups", [])
                cache.invalidate_groups(*groups)
                self._json({"status": "ok", "invalidated": groups})
                return

            fn = self._FUNC_POST_ROUTES.get(path)
            if fn:
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length)) if length else {}
                fn(self, body)  # type: ignore[operator]
                # Invalidate cache groups after successful mutation
                groups = self._POST_INVALIDATIONS.get(path)
                if groups:
                    cache.invalidate_groups(*groups)
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


def main():
    global db, beets_db_path, _beets

    parser = argparse.ArgumentParser(description="Soularr Web UI")
    parser.add_argument("--port", type=int, default=8085)
    parser.add_argument("--dsn", default=os.environ.get("PIPELINE_DB_DSN", "postgresql://soularr@localhost/soularr"))
    parser.add_argument("--beets-db", default="/mnt/virtio/Music/beets-library.db")
    parser.add_argument("--mb-api", default=None, help="MusicBrainz API base URL")
    parser.add_argument("--redis-host", default=None, help="Redis host for caching (optional)")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    if args.redis_host:
        cache.init(args.redis_host, args.redis_port)
        # Flush stale web:* keys so backend changes (e.g. updated discogs.py
        # normalizer) take effect immediately on restart instead of being
        # masked by 24h-TTL MB/discogs entries.
        cache.invalidate_pattern("web:*")

    if args.mb_api:
        mb_api.MB_API_BASE = args.mb_api

    global _db_dsn
    _db_dsn = args.dsn
    db = PipelineDB(args.dsn)
    beets_db_path = args.beets_db
    if beets_db_path and os.path.exists(beets_db_path):
        _beets = BeetsDB(beets_db_path)

    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"Soularr Web UI listening on http://0.0.0.0:{args.port}")
    print(f"  Pipeline DB: {args.dsn}")
    print(f"  Beets DB: {beets_db_path}")
    print(f"  MB API: {mb_api.MB_API_BASE}")
    print(f"  Redis: {args.redis_host or 'disabled'}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()
    _db().close()


if __name__ == "__main__":
    main()
