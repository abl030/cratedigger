#!/usr/bin/env python3
"""Cratedigger Web UI — album request manager at music.ablz.au.

Browse MusicBrainz, add releases to the pipeline DB, view status.

Usage:
    python3 web/server.py --port 8085 --dsn postgresql://cratedigger@192.168.100.11/cratedigger
"""

import os
import sys

# Script-mode Python puts this file's directory (web/) at sys.path[0]
# (production boots `python .../web/server.py` from the systemd wrapper),
# which makes every web module importable under a bare second name
# (`import mb`, `from routes import ...`) — the issue #95 / PR #94 dual-load
# bug class, where
# two copies of the same class break `is` and isinstance across the
# boundary. Strip it (realpath: a symlink-aliased spelling of web/ must
# not survive the filter) before ANY other import so each module has
# exactly one canonical name.
_WEB_DIR = os.path.realpath(os.path.dirname(os.path.abspath(__file__)))
sys.path[:] = [
    p for p in sys.path if os.path.realpath(p or os.getcwd()) != _WEB_DIR
]

# Ensure repo root is importable when run as __main__ so `from lib.X` /
# `from web.X` resolve without relying on PYTHONPATH.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import argparse
import json
import logging
import re
import shutil
import sqlite3
import threading
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("cratedigger-web")

# Ensure this module is importable as 'web.server' even when run as __main__,
# so route modules can `from web import server` and get the same instance.
if __name__ == "__main__" or "web.server" not in sys.modules:
    sys.modules["web.server"] = sys.modules[__name__]

from web import cache as cache
from web import mb as mb_api
from web import overlay as _overlay
from lib.beets_db import BeetsDB
from lib.pipeline_db import PipelineDB
from web.routes import browse as _browse_routes
from web.routes import disk_coverage as _disk_coverage_routes
from web.routes import labels as _labels_routes
from web.routes import library as _library_routes
from web.routes import imports as _imports_routes
from web.routes import pipeline as _pipeline_routes
from web.routes import youtube as _youtube_routes

_db_dsn = None

# Globals set in main() / injected by the test harness and dev server.
# With `_db_dsn` set (production), request threads NEVER touch these —
# each `ThreadingHTTPServer` worker gets its own handles via
# `_thread_state` below, because neither psycopg2 connections nor
# sqlite3 handles are safe to share across threads. With `_db_dsn`
# unset (tests, web_dev_server live-db mode), `db` is the injected
# shared handle and the caller owns its thread-safety.
db: PipelineDB | None = None
beets_db_path: str | None = None
_beets: BeetsDB | None = None

# Per-thread DB handles. Threads are mostly long-lived: the Handler
# speaks HTTP/1.1 keep-alive, so a browser's persistent connections
# each pin one worker thread (and its handles) across many requests.
# One-shot clients (curl, the importer's notify hooks) cost one
# connect/teardown each — fine at single-operator scale.
_thread_state = threading.local()


def _try_reconnect_db():
    """Drop the current thread's pipeline-DB handle so the next
    `_db()` call opens a fresh connection.

    Only request-handler threads call this (from the do_GET/do_POST
    catch-alls), so the thread-local is the right scope; other
    threads' healthy connections are left alone. PipelineDB also
    self-heals via `_ensure_conn`, so this is belt-and-braces for
    errors that escape it."""
    if not _db_dsn:
        return
    handle = getattr(_thread_state, "db", None)
    if handle is not None:
        try:
            handle.conn.close()
        except Exception:
            pass
        _thread_state.db = None
        log.info("Dropped this thread's pipeline DB handle; next request reconnects")


def _db() -> PipelineDB:
    """Return this thread's pipeline DB, opening it on first use."""
    if not _db_dsn:
        # Injected shared handle (test harness / dev server).
        if db is None:
            raise RuntimeError("Pipeline DB not connected")
        return db
    handle = getattr(_thread_state, "db", None)
    if handle is None:
        handle = PipelineDB(_db_dsn)
        _thread_state.db = handle
    return handle


def _new_db() -> PipelineDB:
    """Open a fresh pipeline-DB connection for a background thread.

    Background work that outlives a request (the bulk-triage sweep)
    must not borrow a request thread's handle. Falls back to the shared
    handle when no DSN is configured (test harness — the handler mock
    stands in for both).
    """
    if _db_dsn:
        return PipelineDB(_db_dsn)
    return _db()


def _beets_db() -> BeetsDB | None:
    """Return this thread's BeetsDB, or None if not configured.

    sqlite3 connections are bound to their opening thread
    (`check_same_thread`), so each worker opens its own read-only
    handle on first use. An injected `_beets` (tests) wins."""
    if _beets is not None:
        return _beets
    if not beets_db_path or not os.path.exists(beets_db_path):
        return None
    handle = getattr(_thread_state, "beets", None)
    if handle is None:
        handle = BeetsDB(beets_db_path)
        _thread_state.beets = handle
    return handle


def _close_thread_handles() -> None:
    """Close and drop this thread's DB handles.

    Called from ``Handler.finish()`` — under ``ThreadingHTTPServer``
    one thread serves one connection, so connection-close IS
    thread-death and this releases the psycopg2/sqlite handles
    deterministically instead of waiting on GC (#435). Injected shared
    handles (``db``/``_beets`` — tests, dev server) are never touched."""
    handle = getattr(_thread_state, "db", None)
    if handle is not None:
        try:
            handle.close()
        except Exception:
            pass
        _thread_state.db = None
    beets_handle = getattr(_thread_state, "beets", None)
    if beets_handle is not None:
        try:
            beets_handle.close()
        except Exception:
            pass
        _thread_state.beets = None


# ── Overlay wiring ───────────────────────────────────────────────────
#
# The overlay/domain logic lives in web/overlay.py with explicit DB
# parameters (#432). This module is the composition root: it binds the
# per-thread handles and re-exports the bound names that route modules
# (and test patch targets) consume via ``srv.X``.


def _db_available() -> bool:
    """True when `_db()` can return a handle — a DSN for per-thread
    connections, or an injected shared handle (tests / dev server)."""
    return bool(_db_dsn) or db is not None


def _db_or_none() -> PipelineDB | None:  # noqa: same nominal type, server-owned
    """This thread's pipeline DB, or None when no DB is configured."""
    return _db() if _db_available() else None


# Pure helpers — re-bound so routes / tests keep their existing names.
_serialize_row = _overlay.serialize_row
apply_pipeline_bitrate_override = _overlay.apply_pipeline_bitrate_override
compute_library_rank = _overlay.compute_library_rank


def check_beets_library(mbids: list[str] | list[object]) -> set[str]:
    return _overlay.check_beets_library(_beets_db(), mbids)


def check_beets_library_detail(mbids: list[str] | list[object]) -> dict[str, dict[str, object]]:
    return _overlay.check_beets_library_detail(_beets_db(), mbids)


def get_library_artist(
    artist_name: str,
    mb_artist_id: str = "",
) -> list[dict[str, object]]:
    return _overlay.get_library_artist(_beets_db(), artist_name, mb_artist_id)


def check_pipeline(mbids):
    return _overlay.check_pipeline(_db_or_none(), mbids)


def _enrich_with_pipeline(albums: list[dict[str, object]]) -> None:
    _overlay.enrich_with_pipeline(_db_or_none(), albums)


class Handler(BaseHTTPRequestHandler):

    # HTTP/1.1 keep-alive: a browser's persistent connections each pin
    # one worker thread, so its thread-local DB handles amortize across
    # requests instead of reconnecting per request. Requires every
    # response to carry Content-Length (all writers here do).
    protocol_version = "HTTP/1.1"
    # Reap idle keep-alive threads: a worker blocked waiting for the
    # client's next request gives up after this many seconds, closing
    # the connection and releasing its DB handles.
    timeout = 75

    # Route tables: path → handler function.
    # Route modules export their own dicts; we merge them here.
    _FUNC_GET_ROUTES: dict[str, object] = {
        **_browse_routes.GET_ROUTES,
        **_disk_coverage_routes.GET_ROUTES,
        **_labels_routes.GET_ROUTES,
        **_pipeline_routes.GET_ROUTES,
        **_library_routes.GET_ROUTES,
        **_imports_routes.GET_ROUTES,
        **_youtube_routes.GET_ROUTES,
    }

    _FUNC_GET_PATTERNS: list[tuple[re.Pattern[str], object]] = [
        *_browse_routes.GET_PATTERNS,
        *_disk_coverage_routes.GET_PATTERNS,
        *_labels_routes.GET_PATTERNS,
        *_pipeline_routes.GET_PATTERNS,
        *_library_routes.GET_PATTERNS,
        *_youtube_routes.GET_PATTERNS,
    ]

    _FUNC_POST_ROUTES: dict[str, object] = {
        **_pipeline_routes.POST_ROUTES,
        **_library_routes.POST_ROUTES,
        **_imports_routes.POST_ROUTES,
        **_youtube_routes.POST_ROUTES,
    }

    _FUNC_POST_PATTERNS: list[tuple[re.Pattern[str], object]] = [
        *getattr(_pipeline_routes, "POST_PATTERNS", []),
        *_youtube_routes.POST_PATTERNS,
    ]

    # Description tables (U18): human-readable strings for the route index,
    # mirroring the dispatch-table merge above. Each route module exports
    # parallel `*_DESCRIPTIONS` dicts/lists that start empty and are
    # populated incrementally. Empty until U18 step 2.
    _FUNC_GET_DESCRIPTIONS: dict[str, str] = {
        **_browse_routes.GET_DESCRIPTIONS,
        **_disk_coverage_routes.GET_DESCRIPTIONS,
        **_labels_routes.GET_DESCRIPTIONS,
        **_pipeline_routes.GET_DESCRIPTIONS,
        **_library_routes.GET_DESCRIPTIONS,
        **_imports_routes.GET_DESCRIPTIONS,
        **_youtube_routes.GET_DESCRIPTIONS,
    }

    _FUNC_POST_DESCRIPTIONS: dict[str, str] = {
        **_pipeline_routes.POST_DESCRIPTIONS,
        **_library_routes.POST_DESCRIPTIONS,
        **_imports_routes.POST_DESCRIPTIONS,
        **_youtube_routes.POST_DESCRIPTIONS,
    }

    _FUNC_GET_PATTERN_DESCRIPTIONS: list[tuple[re.Pattern[str], str]] = [
        *_browse_routes.PATTERN_DESCRIPTIONS,
        *_disk_coverage_routes.PATTERN_DESCRIPTIONS,
        *_labels_routes.PATTERN_DESCRIPTIONS,
        *_pipeline_routes.PATTERN_DESCRIPTIONS,
        *_library_routes.PATTERN_DESCRIPTIONS,
        *_youtube_routes.PATTERN_DESCRIPTIONS,
    ]

    # POST pattern descriptions mirror the dispatch table — spread each
    # route module's ``POST_PATTERN_DESCRIPTIONS`` (with getattr so the
    # absence isn't an error). Finding #21: the YT module's list wasn't
    # merged before, leaving its (empty) description list out of the
    # symmetric pattern-description table.
    _FUNC_POST_PATTERN_DESCRIPTIONS: list[tuple[re.Pattern[str], str]] = [
        *getattr(_pipeline_routes, "POST_PATTERN_DESCRIPTIONS", []),
        *getattr(_library_routes, "POST_PATTERN_DESCRIPTIONS", []),
        *getattr(_imports_routes, "POST_PATTERN_DESCRIPTIONS", []),
        *getattr(_youtube_routes, "POST_PATTERN_DESCRIPTIONS", []),
    ]

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

    # Browser icon assets (#161). Allowlist keyed by URL path — no
    # filesystem-derived names, so no traversal surface.
    _STATIC_ASSETS = {
        "/favicon.ico": ("favicon.ico", "image/x-icon"),
        "/favicon-16x16.png": ("favicon-16x16.png", "image/png"),
        "/favicon-32x32.png": ("favicon-32x32.png", "image/png"),
        "/apple-touch-icon.png": ("apple-touch-icon.png", "image/png"),
    }

    def _static_asset(self, url_path):
        """Serve an allowlisted icon asset from web/assets/."""
        filename, content_type = self._STATIC_ASSETS[url_path]
        asset_path = os.path.join(os.path.dirname(__file__), "assets", filename)
        with open(asset_path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "public, max-age=86400")
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

            # Browser icon assets
            if path in self._STATIC_ASSETS:
                self._static_asset(path)
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
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            # Issue #233: client closed mid-response. The original wedge cause
            # was the broad `except Exception` below firing _try_reconnect_db
            # on every disconnect, which compounded into a reconnect storm
            # under sustained client-disconnect traffic. Catch the disconnect
            # classes here first — single warning line, no DB churn, no second
            # body-write attempt to a dead socket.
            log.warning("Client disconnect on GET %s: %s", path, type(e).__name__)
            self.close_connection = True
        except Exception as e:
            log.exception("GET %s failed", path)
            _try_reconnect_db()
            # The handler may have already sent headers or a partial body;
            # under HTTP/1.1 keep-alive a follow-up response on the same
            # socket would desync the stream, so always close after.
            self.close_connection = True
            self._error(str(e), 500)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/")

        try:
            # Cache invalidation endpoint — kept for backwards compat with
            # cratedigger's main-loop POST at end of every cycle. Post-#101
            # there's nothing to invalidate at the `web:` namespace, so
            # this is a best-effort no-op. NOTE: The cratedigger-side caller
            # was deleted in this PR; this handler stays in place to absorb
            # the deploy-window asymmetry (in-flight cycles may POST during
            # the swap). Tracked for cleanup in #234.
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
            for pattern, fn in self._FUNC_POST_PATTERNS:
                m = pattern.match(path)
                if m:
                    length = int(self.headers.get("Content-Length", 0))
                    body = json.loads(self.rfile.read(length)) if length else {}
                    fn(self, body, *m.groups())  # type: ignore[operator]
                    return
            self._error("Not found", 404)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            # Issue #233: see do_GET above. The function-level except covers
            # both the cache-invalidate short-circuit and the dispatch path —
            # they share this outer try block, so any disconnect on either is
            # handled here.
            log.warning("Client disconnect on POST %s: %s", path, type(e).__name__)
            self.close_connection = True
        except Exception as e:
            log.exception("POST %s failed", path)
            _try_reconnect_db()
            # See do_GET: never reuse the socket after an error response.
            self.close_connection = True
            self._error(str(e), 500)

    def finish(self):
        """Connection teardown: release this thread's DB handles.

        Runs once per connection (after the keep-alive loop ends), which
        under ThreadingHTTPServer is the moment the worker thread dies."""
        try:
            super().finish()
        finally:
            _close_thread_handles()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        # HTTP/1.1 keep-alive: a bodyless response must still declare
        # its (zero) length or the client waits for a body forever.
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ── GET handlers ─────────────────────────────────────────────────

    def _get_index(self, params: dict[str, list[str]]) -> None:
        self._html("index.html")


def main():
    global beets_db_path

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
    # Fail fast at boot if the DB is unreachable; request threads open
    # their own handles via `_db()`, so this one is connect-check only.
    PipelineDB(args.dsn).close()
    beets_db_path = args.beets_db
    if beets_db_path and not os.path.exists(beets_db_path):
        log.warning("Beets DB not found at %s; library routes degrade", beets_db_path)

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
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
