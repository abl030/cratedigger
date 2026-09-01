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
# External authorization is an assertion the deployment makes about the
# component in front of the gateway. Cratedigger never contacts that component,
# so this is recorded as an ordinary startup fact, not a warning about a
# missing perimeter.
EXTERNAL_AUTH_NOTICE = (
    "Browser authorization is owned by an external component in front of "
    "this Cratedigger gateway."
)

# Ensure this module is importable as 'web.server' even when run as __main__,
# so `import web.server` elsewhere resolves to this same instance.
if __name__ == "__main__" or "web.server" not in sys.modules:
    sys.modules["web.server"] = sys.modules[__name__]

from lib.beets_startup import BeetsStartupError, enforce_beets_startup
from lib.config import (
    resolve_startup_config_paths,
)
from lib.json_narrow import is_str_object_dict as _is_str_object_dict
from lib.pipeline_db import PipelineDB
from web import cache
from web import discogs as _discogs
from web import mb as mb_api
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
from web.routes import retag_divergence_audit as _retag_divergence_audit_routes
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
from web.runtime import WebRuntime, install_runtime, runtime

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
    _retag_divergence_audit_routes,
)


def announce_insecure_mode(enabled: bool) -> None:
    """Record the explicit insecure-authentication startup decision.

    Holds no state — the flag itself is ``WebRuntime.insecure_mode``,
    read at render time. This is the startup record and nothing else,
    the same shape :func:`announce_external_auth_mode` has always had.
    """
    if enabled:
        log.critical(INSECURE_AUTH_WARNING)


def announce_external_auth_mode(enabled: bool) -> None:
    """Record that an external component owns browser authorization.

    External mode changes nothing this process does — authentication is
    present, it just lives in front of the gateway — so the startup
    record IS the whole behaviour, and a mode flag nothing reads would
    be dead weight.
    """
    if enabled:
        log.info(EXTERNAL_AUTH_NOTICE)


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

    def setup(self) -> None:
        """Bind this connection to the runtime it is served under.

        Resolved once per connection rather than per call, for two
        reasons. It keeps teardown total — ``finish()`` must never raise,
        and a connection accepted with nothing installed (only reachable
        in the shutdown window, since ``main()`` constructs the listener
        *inside* its install block) would otherwise turn every teardown
        into a logged server error. And it keeps ownership straight: the
        handles ``drop_thread_db`` and ``close_thread_handles`` release
        are the ones *this* connection's runtime opened, not whichever
        runtime happens to be installed by the time it ends. Route
        handlers deliberately still call ``runtime()`` themselves, so a
        test that nests an install around a request sees the nested one.
        """
        super().setup()
        try:
            self._runtime: WebRuntime | None = runtime()
        except RuntimeError:
            self._runtime = None

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
            # Decided before the runtime is consulted, and teardown is
            # total (see setup()), so liveness answers end to end with
            # nothing configured at all.
            return True
        origin = self._runtime.canonical_origin if self._runtime else None
        if origin is None:
            self.close_connection = True
            self._error("Request rejected", 403)
            return False
        try:
            authorize_request(
                method=self.command,
                channel_values=self.headers.get_all(CHANNEL_HEADER, []),
                origin_values=self.headers.get_all("Origin", []),
                referer_values=self.headers.get_all("Referer", []),
                canonical_origin=origin,
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
            body = _rendered_index_document(runtime().insecure_mode)
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
            # was the broad `except Exception` below firing the reconnect
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
            if self._runtime is not None:
                self._runtime.drop_thread_db()
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
            if self._runtime is not None:
                self._runtime.drop_thread_db()
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
            if self._runtime is not None:
                self._runtime.close_thread_handles()

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
        "--external-auth-mode",
        action="store_true",
        help=(
            "Record that an established external component in front of this "
            "gateway owns browser authorization. Cratedigger performs no "
            "authorization and contacts no authorizer; the request-security "
            "envelope remains enforced."
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

    # Startup write-probe (issue #1085): fail loudly, before any queue
    # recovery, claim, DB mutation, or filesystem mutation, if a required
    # path cannot be used the way this unit is about to use it.
    from lib.startup_write_probe import (
        StartupProbeError,
        probe_startup_paths,
        web_required_paths,
    )
    required_paths = web_required_paths(admitted_config)
    try:
        probe_startup_paths(
            unit="cratedigger-web",
            logger=log,
            required=required_paths,
        )
    except StartupProbeError:
        return 1

    # The distance modules above import Beets eagerly. Rebind their shared
    # LazyConfig only after admission so they cannot retain a caller's
    # inherited BEETSDIR authority.
    from beets import config as active_beets_config
    active_beets_config.clear()
    active_beets_config.read(user=True, defaults=True)
    if args.insecure_mode and args.external_auth_mode:
        parser.error(
            "--insecure-mode and --external-auth-mode are mutually exclusive"
        )
    announce_insecure_mode(args.insecure_mode)
    announce_external_auth_mode(args.external_auth_mode)
    from lib.library_completeness_snapshot import (
        library_completeness_snapshot_path as completeness_snapshot_path,
    )
    from lib.retag_divergence_census_snapshot import (
        retag_divergence_census_snapshot_path,
    )
    # Everything this process's routes read, in one value, built once
    # from the admitted config. Nothing before this point may read it;
    # nothing after may rebind it.
    web_runtime = WebRuntime(
        canonical_origin=args.canonical_origin,
        insecure_mode=args.insecure_mode,
        db_dsn=args.dsn,
        beets_db_path=admitted_config.beets_library_db,
        beets_library_root=admitted_config.beets_directory,
        retag_census_snapshot_path=retag_divergence_census_snapshot_path(
            admitted_config.var_dir,
        ),
        library_completeness_snapshot_path=completeness_snapshot_path(
            admitted_config.var_dir,
        ),
    )
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

    # MusicBrainz merge-survivor resolution (#1089): the web server is the
    # second of the two processes that reach a merge seam — the operator
    # merge-rekey route, via ``MergeRekeyService``. Shared with the importer
    # so neither silently drifts; see ``lib/mb_canonical.py``.
    from lib.mb_canonical import configure_canonical_release_lookup
    configure_canonical_release_lookup(admitted_config)

    # Fail fast at boot if the DB is unreachable; request threads open
    # their own handles via ``WebRuntime.db``, so this is connect-check only.
    PipelineDB(args.dsn).close()
    beets_db_path = admitted_config.beets_library_db
    if not os.path.exists(beets_db_path):
        log.warning("Beets DB not found at %s; library routes degrade", beets_db_path)

    # Installed before the listener starts: ``Handler.parse_request``
    # reads ``canonical_origin`` on the first byte of the first request.
    with install_runtime(web_runtime):
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
        # Release whatever handles this (main) thread happens to hold.
        # The old ``_db().close()`` here opened a brand-new connection
        # purely to close it under a DSN, and closed the *caller's*
        # injected handle without one — the opposite of the documented
        # "injected handles are never touched" contract.
        web_runtime.close_thread_handles()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
