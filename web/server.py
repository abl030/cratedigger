#!/usr/bin/env python3
"""Cratedigger Web UI — album request manager at music.ablz.au.

Browse MusicBrainz, add releases to the pipeline DB, view status.

Usage:
    python3 web/server.py --port 8085 --dsn postgresql://cratedigger@192.168.100.11/cratedigger
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
log = logging.getLogger("cratedigger-web")

# Ensure repo root is importable when run as __main__ so `from lib.X` /
# `from web.X` resolve without relying on PYTHONPATH.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Ensure this module is importable as 'web.server' even when run as __main__,
# so route modules can `from web import server` and get the same instance.
if __name__ == "__main__" or "web.server" not in sys.modules:
    sys.modules["web.server"] = sys.modules[__name__]

from web import cache as cache
from web import mb as mb_api
from lib.beets_db import BeetsDB
from lib.pipeline_db import PipelineDB
from web.routes import browse as _browse_routes
from web.routes import library as _library_routes
from web.routes import imports as _imports_routes
from web.routes import pipeline as _pipeline_routes

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


_rank_cfg_cache = None


def _rank_cfg():
    """Cached QualityRankConfig from runtime config.ini.

    Falls back to defaults if the ini can't be read (e.g. tests / first-
    boot). The cache is module-scoped — a deploy restart picks up any
    [Quality Ranks] changes via the cratedigger-web service restart that
    deploy.md guarantees.
    """
    global _rank_cfg_cache
    if _rank_cfg_cache is None:
        try:
            from lib.config import read_runtime_rank_config
            _rank_cfg_cache = read_runtime_rank_config()
        except Exception:
            from lib.quality import QualityRankConfig
            _rank_cfg_cache = QualityRankConfig.defaults()
    return _rank_cfg_cache


def compute_library_rank(format_str: str | None, bitrate_kbps: int | None) -> str:
    """Codec-aware quality rank label for a beets album.

    Single source of truth for the in-library badge's tier — same logic
    the import gate uses, so what you see in the badge matches what the
    pipeline's quality decisions act on. Returns lowercase rank name
    ('lossless', 'transparent', 'excellent', 'good', 'acceptable',
    'poor', 'unknown'). Treats MP3 as VBR — cratedigger's pipeline only
    produces VBR-V0 MP3, and for the bitrate buckets the badge cares
    about the VBR-vs-CBR distinction barely matters at the display level.
    """
    if not format_str:
        return "unknown"
    fmt = format_str.split(",")[0].strip()
    if not fmt:
        return "unknown"
    from lib.quality import quality_rank
    rank = quality_rank(fmt, bitrate_kbps, is_cbr=False, cfg=_rank_cfg())
    return rank.name.lower()


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

    # Routing-level response cache was removed by issue #101 — it used to
    # cache the full HTTP response under `web:<url>`, which baked in
    # per-request overlay state (pipeline_status, in_library, …) and
    # leaked stale badges for up to 5 min after cratedigger-the-pipeline
    # wrote to Postgres outside the web UI's POST paths.
    #
    # The pure MB/Discogs metadata that this cache used to cover is now
    # memoized one layer down, inside web/mb.py and web/discogs.py, at
    # the `meta:` namespace (24h TTL). Local-DB overlays (check_pipeline,
    # check_beets_library) run on every request — cheap single-SQL
    # lookups that no longer need caching.
    #
    # `cache.invalidate_groups()` is still callable for backwards
    # compatibility with cratedigger's main loop POSTing to
    # /api/cache/invalidate, but it's a no-op for any fresh deploy
    # (no `web:` keys exist).

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)

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

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            # Cache invalidation endpoint — kept for backwards compat with
            # cratedigger's main-loop POST at end of every cycle. Post-#101
            # there's nothing to invalidate at the `web:` namespace, so
            # this is a best-effort no-op.
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

    parser = argparse.ArgumentParser(description="Cratedigger Web UI")
    parser.add_argument("--port", type=int, default=8085)
    parser.add_argument("--dsn", default=os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger"))
    parser.add_argument("--beets-db", default="/mnt/virtio/Music/beets-library.db")
    parser.add_argument("--mb-api", default=None, help="MusicBrainz API base URL")
    parser.add_argument("--redis-host", default=None, help="Redis host for caching (optional)")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    if args.redis_host:
        cache.init(args.redis_host, args.redis_port)
        # Flush only the legacy `web:*` routing namespace on startup. It
        # was removed in #101 but may still hold stale overlay-baked
        # responses on in-place upgrades.
        #
        # Do NOT flush `meta:*` here — it's the 24h pure-metadata cache
        # that should survive routine restarts (Codex review). If a
        # helper-shape change needs to invalidate cached metadata (rare
        # — e.g. a discogs.py normalizer tweak), bump the cache key
        # prefix in the helper or flush `meta:*` manually during deploy.
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
    print(f"Cratedigger Web UI listening on http://0.0.0.0:{args.port}")
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
