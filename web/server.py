"""Cratedigger Web UI — album request manager at music.ablz.au.

Browse MusicBrainz, add releases to the pipeline DB, view status.

Usage:
    python3 web/server.py --canonical-origin https://music.example \
        --dev-port 8085 --dsn postgresql://cratedigger@10.20.0.11/cratedigger
"""
import os
import socket
import socketserver
import sys
from typing import ClassVar

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
import functools
import json
import logging
import re
import threading
from collections.abc import Callable, Mapping
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import msgspec

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
log = logging.getLogger("cratedigger-web")

MAX_POST_BODY_BYTES = 1024 * 1024
INSECURE_AUTH_WARNING = (
    "Authentication is disabled for this Cratedigger instance."
)

# Ensure this module is importable as 'web.server' even when run as __main__,
# so route modules can `from web import server` and get the same instance.
if __name__ == "__main__" or "web.server" not in sys.modules:
    sys.modules["web.server"] = sys.modules[__name__]

from lib.beets_db import BeetsDB, open_beets_db
from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import (
    resolve_startup_config_paths,
)
from lib.json_narrow import is_str_object_dict as _is_str_object_dict
from lib.pipeline_db import AlbumRequestRow, PipelineDB
from web import cache
from web import discogs as _discogs
from web import mb as mb_api
from web import overlay as _overlay
from web.index_document import render_index_document
from web.request_security import (
    CHANNEL_HEADER,
    RequestSecurityError,
    authorize_request,
    is_exact_liveness_request,
    validate_canonical_origin,
)
from web.routes import api_index as _api_index_routes
from web.routes import beets_distance as _beets_distance_routes
from web.routes import browse as _browse_routes
from web.routes import disk_coverage as _disk_coverage_routes
from web.routes import health as _health_routes
from web.routes import imports as _imports_routes
from web.routes import labels as _labels_routes
from web.routes import library as _library_routes
from web.routes import long_tail as _long_tail_routes
from web.routes import pipeline as _pipeline_routes
from web.routes import pipeline_dashboard as _pipeline_dashboard_routes
from web.routes import pipeline_mutations as _pipeline_mutations_routes
from web.routes import release_identity_routes as _release_identity_routes
from web.routes import search_plan as _search_plan_routes
from web.routes import triage as _triage_routes
from web.routes import world_audit as _world_audit_routes
from web.routes import youtube as _youtube_routes
from web.routes._registry import (
    RouteRegistration,
    build_get_patterns,
    build_get_routes,
    build_post_patterns,
    build_post_routes,
    merge_registries,
)

# Single merged registry (#496): each route module exports one
# ``ROUTES: list[RouteRegistration]`` next to its handlers; this is the
# one place they're combined. ``Handler``'s dispatch tables below are
# derived views over this list — not separately maintained structures.
ALL_ROUTES: list[RouteRegistration] = merge_registries(
    _api_index_routes,
    _beets_distance_routes,
    _browse_routes,
    _disk_coverage_routes,
    _labels_routes,
    _long_tail_routes,
    _pipeline_routes,
    _pipeline_dashboard_routes,
    _pipeline_mutations_routes,
    _release_identity_routes,
    _library_routes,
    _imports_routes,
    _search_plan_routes,
    _triage_routes,
    _youtube_routes,
    _world_audit_routes,
)

_db_dsn = None
canonical_origin: str | None = None
insecure_mode = False

# Globals set in main() / injected by the test harness and dev server.
# With `_db_dsn` set (production), request threads NEVER touch these —
# each threaded HTTP worker gets its own handles via
# `_thread_state` below, because neither psycopg2 connections nor
# sqlite3 handles are safe to share across threads. With `_db_dsn`
# unset (tests, web_dev_server live-db mode), `db` is the injected
# shared handle and the caller owns its thread-safety.
db: PipelineDB | None = None
# Explicit dev/test override only. Production leaves this unset and opens the
# runtime [Beets] library+directory pair through ``open_beets_db``.
beets_db_path: str | None = None
beets_library_root: str = ""
_beets: BeetsDB | None = None
# Explicit test/dev dependency-injection seams for the pinned destructive
# operation. Production leaves both unset and the service selects its real
# subprocess/notifier implementations.
beets_delete_fn = None
delete_notify_fn = None

# Per-thread DB handles. Threads are mostly long-lived: the Handler
# speaks HTTP/1.1 keep-alive, so a browser's persistent connections
# each pin one worker thread (and its handles) across many requests.
# One-shot clients (curl, the importer's notify hooks) cost one
# connect/teardown each — fine at single-operator scale.
_thread_state = threading.local()


def configure_insecure_mode(enabled: bool) -> None:
    """Select insecure presentation and log the explicit startup decision."""
    global insecure_mode

    insecure_mode = enabled
    if enabled:
        log.critical(INSECURE_AUTH_WARNING)


@functools.cache
def _rendered_index_document(insecure: bool) -> bytes:
    """Read and validate the immutable production index once per auth mode."""
    html_path = os.path.join(os.path.dirname(__file__), "index.html")
    with open(html_path, "rb") as handle:
        return render_index_document(handle.read(), insecure=insecure)


class ThreadingUnixHTTPServer(
    socketserver.ThreadingMixIn,
    socketserver.UnixStreamServer,
):
    """Thread-per-connection HTTP server over one adopted Unix listener."""

    daemon_threads = True
    server_name: str
    server_port: int

    def __init__(
        self,
        listener: socket.socket,
        handler_class: type[BaseHTTPRequestHandler],
    ) -> None:
        # The socket is already bound/listening under systemd ownership.
        # BaseServer initializes shutdown/threading state without creating,
        # binding, or activating a second listener.
        socketserver.BaseServer.__init__(
            self,
            listener.getsockname(),
            handler_class,
        )
        self.socket = listener
        self.server_name = "cratedigger.internal"
        self.server_port = 0


def _take_systemd_unix_listener(
    *,
    environ: Mapping[str, str] | None = None,
    inherited_fd: int = 3,
) -> socket.socket:
    """Adopt systemd's sole listening AF_UNIX stream fd or fail closed."""
    source = os.environ if environ is None else environ
    if source.get("LISTEN_FDS") != "1":
        raise RuntimeError("production requires exactly one inherited socket")
    if source.get("LISTEN_PID") != str(os.getpid()):
        raise RuntimeError("inherited socket does not belong to current process")
    try:
        listener = socket.socket(fileno=inherited_fd)
    except OSError as exc:
        raise RuntimeError("could not adopt inherited socket") from exc
    try:
        if listener.family != socket.AF_UNIX:
            raise RuntimeError("inherited socket must use AF_UNIX")
        if (
            listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
            != socket.SOCK_STREAM
        ):
            raise RuntimeError("inherited AF_UNIX socket must be a stream")
        if not listener.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN):
            raise RuntimeError("inherited AF_UNIX stream must be listening")
        # systemd passes activation fds across this exec. The web process
        # spawns helpers on some routes, so close the listener at every later
        # exec boundary rather than leaking backend authority into children.
        listener.set_inheritable(False)
    except (OSError, RuntimeError):
        listener.close()
        raise
    return listener


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
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
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
    # Startup installs the exact admitted pair. An absent pair is deliberate
    # dependency injection (tests/dev) and must never trigger a second runtime
    # config read after the one startup admission.
    if beets_db_path is None:
        return None
    handle = getattr(_thread_state, "beets", None)
    if handle is None:
        try:
            handle = open_beets_db(
                db_path=beets_db_path,
                library_root=beets_library_root,
            )
        except FileNotFoundError:
            return None
        _thread_state.beets = handle
    return handle


def _close_thread_handles() -> None:
    """Close and drop this thread's DB handles.

    Called from ``Handler.finish()`` — under either threaded HTTP server
    one thread serves one connection, so connection-close IS
    thread-death and this releases the psycopg2/sqlite handles
    deterministically instead of waiting on GC (#435). Injected shared
    handles (``db``/``_beets`` — tests, dev server) are never touched."""
    handle = getattr(_thread_state, "db", None)
    if handle is not None:
        try:
            handle.close()
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
            pass
        _thread_state.db = None
    beets_handle = getattr(_thread_state, "beets", None)
    if beets_handle is not None:
        try:
            beets_handle.close()
        except Exception:  # noqa: BLE001, S110 - best-effort boundary must not mask primary work
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


# Same nominal type, but this adapter is owned by the server composition root.
def _db_or_none() -> PipelineDB | None:
    """This thread's pipeline DB, or None when no DB is configured."""
    return _db() if _db_available() else None


# Pure helpers — re-bound so routes / tests keep their existing names.
_serialize_row = _overlay.serialize_row
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


def check_pipeline(
    mbids: list[str] | list[object],
) -> dict[str, dict[str, object]]:
    return _overlay.check_pipeline(_db_or_none(), mbids)


def list_artist_requests(
    artist_name: str,
    mb_artist_id: str = "",
) -> list[AlbumRequestRow]:
    """One artist's request rows, for the rg-row badge overlay (#575)."""
    db = _db_or_none()
    if db is None or not artist_name:
        return []
    return db.list_requests_by_artist(artist_name, mb_artist_id)


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

    # Route tables: path → handler function. #496: derived views over the
    # single merged ``ALL_ROUTES`` registry above — not separately
    # maintained structures. Descriptions and contract classification
    # live on each ``RouteRegistration`` itself; see ``ALL_ROUTES``,
    # ``get_api_index`` (web/routes/api_index.py), and
    # ``TestRouteContractAudit`` (tests/web/test_route_audit.py).
    _FUNC_GET_ROUTES: dict[str, Callable[..., None]] = (
        build_get_routes(ALL_ROUTES))
    _FUNC_GET_PATTERNS: list[tuple[re.Pattern[str], Callable[..., None]]] = (
        build_get_patterns(ALL_ROUTES))
    _FUNC_POST_ROUTES: dict[str, Callable[..., None]] = (
        build_post_routes(ALL_ROUTES))
    _FUNC_POST_PATTERNS: list[tuple[re.Pattern[str], Callable[..., None]]] = (
        build_post_patterns(ALL_ROUTES))

    def log_message(self, format: str, *args: object) -> None:
        log.info(format % args)

    def parse_request(self) -> bool:
        """Apply the channel/origin boundary before method dispatch."""
        if not super().parse_request():
            return False
        request_line_parts = self.requestline.split()
        raw_request_target = (
            request_line_parts[1] if len(request_line_parts) >= 2 else ""
        )
        self._security_request_target = raw_request_target
        if is_exact_liveness_request(self.command, raw_request_target):
            return True
        if canonical_origin is None:
            self.close_connection = True
            self._error("Request rejected", 403)
            return False
        try:
            authorize_request(
                method=self.command,
                channel_values=self.headers.get_all(CHANNEL_HEADER, []),
                origin_values=self.headers.get_all("Origin", []),
                referer_values=self.headers.get_all("Referer", []),
                canonical_origin=canonical_origin,
            )
        except RequestSecurityError:
            # A rejected request may carry an unread body. Closing guarantees
            # those bytes cannot be reparsed as another HTTP request.
            self.close_connection = True
            self._error("Request rejected", 403)
            return False
        return True

    def _json(self, data: object, status: int = 200) -> None:
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, path: str) -> None:
        if path == "index.html":
            body = _rendered_index_document(insecure_mode)
        else:
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
    _STATIC_ASSETS: ClassVar = {
        "/favicon.ico": ("favicon.ico", "image/x-icon"),
        "/favicon-16x16.png": ("favicon-16x16.png", "image/png"),
        "/favicon-32x32.png": ("favicon-32x32.png", "image/png"),
        "/apple-touch-icon.png": ("apple-touch-icon.png", "image/png"),
    }

    def _static_asset(self, url_path: str) -> None:
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

    def _static_js(self, path: str) -> None:
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

    def _error(self, msg: str, status: int = 400) -> None:
        self._json({"error": msg}, status)

    def _read_post_body(self) -> dict[str, object] | None:
        """Read one bounded JSON-object POST body, or write its error response.

        ``Content-Length`` is checked before touching ``rfile`` so a client
        cannot make the server buffer an unbounded or syntactically invalid
        request body. An omitted length retains the existing empty-object
        POST behaviour.
        """
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            return {}
        if not raw_length.isascii() or not raw_length.isdecimal():
            self.close_connection = True
            self._error("Invalid Content-Length")
            return None
        normalized_length = raw_length.lstrip("0") or "0"
        if len(normalized_length) > len(str(MAX_POST_BODY_BYTES)):
            self.close_connection = True
            self._error("Request body too large", 413)
            return None
        length = int(normalized_length)
        if length > MAX_POST_BODY_BYTES:
            self.close_connection = True
            self._error("Request body too large", 413)
            return None
        body: object = json.loads(self.rfile.read(length)) if length else {}
        if not _is_str_object_dict(body):
            self._error("Request body must be a JSON object")
            return None
        return body

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
        request_target = getattr(
            self, "_security_request_target", self.path,
        )
        if is_exact_liveness_request(self.command, request_target):
            _health_routes.serve_healthz(self)
            return
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
                fn(self, params)
                return
            for pattern, fn in self._FUNC_GET_PATTERNS:
                m = pattern.match(path)
                if m:
                    fn(self, params, *m.groups())
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
        except _discogs.DiscogsMirrorNotConfigured as e:
            # Deliberate config posture, not a crash: no Discogs mirror ->
            # Discogs browse is off (tier-2 plan R13). 503 with the
            # actionable message, no DB reconnect churn.
            self._error(str(e), 503)
        except Exception:
            log.exception("GET %s failed", path)
            _try_reconnect_db()
            # The handler may have already sent headers or a partial body;
            # under HTTP/1.1 keep-alive a follow-up response on the same
            # socket would desync the stream, so always close after.
            self.close_connection = True
            self._error("Internal server error", 500)

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
                body = self._read_post_body()
                if body is None:
                    return
                try:
                    groups = msgspec.convert(
                        body.get("groups", []), type=list[str])
                except msgspec.ValidationError:
                    groups = []
                cache.invalidate_groups(*groups)
                self._json({"status": "ok", "invalidated": groups})
                return

            fn = self._FUNC_POST_ROUTES.get(path)
            if fn:
                body = self._read_post_body()
                if body is None:
                    return
                fn(self, body)
                return
            for pattern, fn in self._FUNC_POST_PATTERNS:
                m = pattern.match(path)
                if m:
                    body = self._read_post_body()
                    if body is None:
                        return
                    fn(self, body, *m.groups())
                    return
            # The declared body remains unread for an unknown route.  Close
            # rather than letting those bytes be parsed as a second request.
            self.close_connection = True
            self._error("Not found", 404)
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError) as e:
            # Issue #233: see do_GET above. The function-level except covers
            # both the cache-invalidate short-circuit and the dispatch path —
            # they share this outer try block, so any disconnect on either is
            # handled here.
            log.warning("Client disconnect on POST %s: %s", path, type(e).__name__)
            self.close_connection = True
        except _discogs.DiscogsMirrorNotConfigured as e:
            # Deliberate config posture (no Discogs mirror) — clean 503,
            # no DB reconnect churn (R13).
            self._error(str(e), 503)
        except Exception:
            log.exception("POST %s failed", path)
            _try_reconnect_db()
            # See do_GET: never reuse the socket after an error response.
            self.close_connection = True
            self._error("Internal server error", 500)

    def finish(self):
        """Connection teardown: release this thread's DB handles.

        Runs once per connection (after the keep-alive loop ends), which
        under the threaded server is the moment the worker thread dies."""
        try:
            super().finish()
        finally:
            _close_thread_handles()

    def do_HEAD(self) -> None:
        request_target = getattr(
            self, "_security_request_target", self.path,
        )
        if is_exact_liveness_request(self.command, request_target):
            _health_routes.serve_healthz(self)
            return
        self.send_error(501, "Unsupported method")

    def do_OPTIONS(self):
        self.send_response(200)
        # HTTP/1.1 keep-alive: a bodyless response must still declare
        # its (zero) length or the client waits for a body forever.
        self.send_header("Content-Length", "0")
        self.end_headers()

    # ── GET handlers ─────────────────────────────────────────────────

    def _get_index(self, params: dict[str, list[str]]) -> None:
        self._html("index.html")


def main() -> int:
    global beets_db_path, beets_library_root, canonical_origin

    parser = argparse.ArgumentParser(description="Cratedigger Web UI")
    parser.add_argument(
        "--dev-port",
        type=int,
        default=None,
        help=(
            "INSECURE DEVELOPMENT ONLY: listen on IPv4 loopback TCP instead "
            "of requiring one systemd-provided Unix socket."
        ),
    )
    parser.add_argument("--dsn", default=os.environ.get("PIPELINE_DB_DSN", "postgresql://cratedigger@localhost/cratedigger"))
    parser.add_argument(
        "--canonical-origin",
        default=os.environ.get("CRATEDIGGER_CANONICAL_ORIGIN"),
        help=(
            "Exact public HTTP(S) origin used for browser mutation "
            "provenance. Required."
        ),
    )
    parser.add_argument(
        "--insecure-mode",
        action="store_true",
        help=(
            "Render and log the explicit insecure-authentication warning. "
            "The request-security envelope remains enforced."
        ),
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Immutable runtime config (default: env or cwd/config.ini)",
    )
    parser.add_argument(
        "--runtime-dir",
        default=None,
        help="Mutable runtime directory (default: cwd)",
    )
    parser.add_argument("--mb-api", default=None,
                        help="MusicBrainz API base URL (full base incl. /ws/2). Dev-only override — "
                             "production reads config.ini [MusicBrainz] api_base (issue #497); the "
                             "NixOS module does not pass this flag.")
    parser.add_argument("--discogs-api", default=None,
                        help="Discogs mirror base URL (mirror-required; unset = Discogs browse off). "
                             "Dev-only override — production reads config.ini [Discogs] api_base "
                             "(issue #497); the NixOS module does not pass this flag.")
    parser.add_argument("--redis-host", default=None, help="Redis host for caching (optional)")
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()
    config_path, runtime_dir = resolve_startup_config_paths(
        config_path=args.config,
        runtime_dir=args.runtime_dir,
    )
    if args.canonical_origin is None:
        parser.error("--canonical-origin is required")
    try:
        validate_canonical_origin(args.canonical_origin)
    except RequestSecurityError as exc:
        parser.error(f"invalid --canonical-origin: {exc}")
    try:
        admitted_config = enforce_beets_startup(
            role="web",
            config_path=config_path,
            runtime_dir=runtime_dir,
            logger=log,
        )
    except BeetsStartupError:
        return 1

    # The distance modules above import Beets eagerly. Rebind their shared
    # LazyConfig only after admission so they cannot retain a caller's
    # inherited BEETSDIR authority.
    from beets import config as active_beets_config
    active_beets_config.clear()
    active_beets_config.read(user=True, defaults=True)
    canonical_origin = args.canonical_origin
    configure_insecure_mode(args.insecure_mode)
    beets_db_path = admitted_config.beets_library_db
    beets_library_root = admitted_config.beets_directory
    inherited_listener: socket.socket | None = None
    if args.dev_port is None:
        try:
            inherited_listener = _take_systemd_unix_listener()
        except RuntimeError as exc:
            parser.error(str(exc))

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

    # API bases: config.ini is the ONE production source (issue #497 —
    # the NixOS module no longer passes --mb-api/--discogs-api). Runtime
    # config is read first (shared startup wiring — the same call
    # pipeline-cli and the youtube worker make); the flags are a dev-only
    # override for a manual invocation and win when set.
    # Config carries ORIGINS; the flag carries the full MB base incl.
    # /ws/2 (KTD6). Discogs stays unset without a mirror — web/discogs.py
    # then serves a clear 503 mirror-required (R13).
    from web.api_bases import configure_api_bases_from_runtime_config
    configure_api_bases_from_runtime_config()
    if args.mb_api:
        mb_api.MB_API_BASE = args.mb_api
    if args.discogs_api:
        _discogs.DISCOGS_API_BASE = args.discogs_api

    global _db_dsn
    _db_dsn = args.dsn
    # Fail fast at boot if the DB is unreachable; request threads open
    # their own handles via `_db()`, so this one is connect-check only.
    PipelineDB(args.dsn).close()
    if not os.path.exists(beets_db_path):
        log.warning("Beets DB not found at %s; library routes degrade", beets_db_path)

    if inherited_listener is None:
        dev_port = args.dev_port
        if not isinstance(dev_port, int):
            raise RuntimeError("development listener port was not selected")
        log.critical(
            "INSECURE DEVELOPMENT TCP listener enabled on 127.0.0.1:%s",
            dev_port,
        )
        server = ThreadingHTTPServer(("127.0.0.1", dev_port), Handler)
        listener_display = f"http://127.0.0.1:{dev_port}"
    else:
        server = ThreadingUnixHTTPServer(inherited_listener, Handler)
        listener_display = f"unix:{inherited_listener.getsockname()}"
    print(f"Cratedigger Web UI listening on {listener_display}")
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
