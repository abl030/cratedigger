#!/usr/bin/env python3
"""Local live-reload server for the Cratedigger web UI.

Serves the checked-out ``web/`` frontend files while backing ``/api/*`` from
fixtures, the production API, or local route code against a live read-only DB.
"""

from __future__ import annotations

import argparse
import html
import json
import mimetypes
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse


REPO_ROOT = Path(__file__).resolve().parents[1]
WEB_ROOT = REPO_ROOT / "web"
FIXTURE_ROOT = REPO_ROOT / "tests" / "fixtures" / "web"
PROD_BASE_URL = "https://music.ablz.au"

sys.path.insert(0, str(REPO_ROOT))


FALLBACK_FIXTURES: dict[str, dict[str, Any]] = {
    "/api/pipeline/all": {
        "counts": {"wanted": 0, "downloading": 0, "imported": 0, "manual": 0},
        "wanted": [],
        "downloading": [],
        "imported": [],
        "manual": [],
    },
    "/api/pipeline/status": {
        "counts": {"wanted": 0, "downloading": 0, "imported": 0, "manual": 0},
        "wanted": [],
    },
    "/api/pipeline/log": {
        "log": [],
        "counts": {
            "all": 0,
            "imported": 0,
            "rejected": 0,
            "matches_24h": 0,
            "matches_6h": 0,
            "matches_per_hour_24h": 0,
            "matches_per_hour_6h": 0,
        },
    },
}


@dataclass(frozen=True)
class DevConfig:
    data: str
    scenario: str
    prod_base_url: str
    dsn: str | None
    beets_db: str | None
    redis_host: str | None
    redis_port: int

    @property
    def badge_text(self) -> str:
        if self.data == "fixture":
            return f"DEV fixture:{self.scenario}"
        if self.data == "prod-api":
            return "DEV prod-api readonly"
        return "DEV live-db readonly"


class DevHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address, RequestHandlerClass, config: DevConfig):
        super().__init__(server_address, RequestHandlerClass)
        self.config = config

    def watched_files(self) -> list[Path]:
        paths = [WEB_ROOT / "index.html"]
        paths.extend(sorted((WEB_ROOT / "js").glob("*.js")))
        scenario_dir = FIXTURE_ROOT / self.config.scenario
        if scenario_dir.is_dir():
            paths.extend(sorted(scenario_dir.glob("*.json")))
        default_dir = FIXTURE_ROOT / "default"
        if default_dir.is_dir() and default_dir != scenario_dir:
            paths.extend(sorted(default_dir.glob("*.json")))
        return paths

    def watch_version(self) -> int:
        version = 0
        for path in self.watched_files():
            try:
                version = max(version, path.stat().st_mtime_ns)
            except FileNotFoundError:
                continue
        return version


class DevHandler(BaseHTTPRequestHandler):
    server: DevHTTPServer

    def log_message(self, fmt: str, *args: object) -> None:
        print(f"{self.address_string()} - {fmt % args}", flush=True)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/__dev/events":
            self._serve_events()
            return
        if path.startswith("/api/"):
            self._serve_api_get(parsed)
            return
        if path == "/favicon.ico":
            self.send_response(204)
            self.end_headers()
            return
        self._serve_static(path)

    def do_POST(self) -> None:
        if urlparse(self.path).path.startswith("/api/"):
            self._json(
                {
                    "error": (
                        "Mutating API requests are blocked by "
                        "scripts/web_dev_server.py"
                    ),
                    "mode": self.server.config.data,
                },
                status=405,
            )
            return
        self._error("Not found", 404)

    def do_OPTIONS(self) -> None:
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _serve_static(self, path: str) -> None:
        rel = "index.html" if path in ("", "/") else path.lstrip("/")
        target = (WEB_ROOT / rel).resolve()
        try:
            target.relative_to(WEB_ROOT.resolve())
        except ValueError:
            self._error("Not found", 404)
            return
        if not target.is_file():
            self._error("Not found", 404)
            return

        if target.name == "index.html":
            body = target.read_text(encoding="utf-8")
            body = body.replace("</body>", f"{self._dev_injection()}</body>")
            self._send_bytes(
                body.encode("utf-8"),
                "text/html; charset=utf-8",
                cache_control="no-cache",
            )
            return

        content_type = mimetypes.guess_type(target.name)[0]
        if target.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        self._send_bytes(
            target.read_bytes(),
            content_type or "application/octet-stream",
            cache_control="no-cache",
        )

    def _serve_api_get(self, parsed) -> None:
        mode = self.server.config.data
        if mode == "fixture":
            self._serve_fixture_api(parsed.path)
        elif mode == "prod-api":
            self._proxy_api_get()
        elif mode == "live-db":
            self._serve_live_db_get(parsed)
        else:
            self._error(f"Unknown data mode: {mode}", 500)

    def _serve_fixture_api(self, path: str) -> None:
        for scenario in (self.server.config.scenario, "default"):
            fixture = FIXTURE_ROOT / scenario / f"{_api_fixture_slug(path)}.json"
            if fixture.is_file():
                self._send_bytes(
                    fixture.read_bytes(),
                    "application/json; charset=utf-8",
                    cache_control="no-cache",
                )
                return
        fallback = FALLBACK_FIXTURES.get(path)
        if fallback is not None:
            self._json(fallback)
            return
        self._json(
            {
                "error": "No fixture for API route",
                "path": path,
                "scenario": self.server.config.scenario,
            },
            status=404,
        )

    def _proxy_api_get(self) -> None:
        url = self.server.config.prod_base_url.rstrip("/") + self.path
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json,*/*",
                "User-Agent": "cratedigger-web-dev-server/1.0",
            },
            method="GET",
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = resp.read()
                content_type = resp.headers.get("Content-Type", "application/json")
                self._send_bytes(body, content_type, status=resp.status)
        except urllib.error.HTTPError as exc:
            self._send_bytes(
                exc.read(),
                exc.headers.get("Content-Type", "application/json"),
                status=exc.code,
            )
        except Exception as exc:
            self._json({"error": str(exc), "upstream": url}, status=502)

    def _serve_live_db_get(self, parsed) -> None:
        import web.server as web_server

        path = parsed.path.rstrip("/") or "/"
        params = parse_qs(parsed.query)
        try:
            fn = web_server.Handler._FUNC_GET_ROUTES.get(path)
            if fn:
                fn(self, params)  # type: ignore[operator]
                return
            for pattern, fn in web_server.Handler._FUNC_GET_PATTERNS:
                match = pattern.match(path)
                if match:
                    fn(self, params, *match.groups())  # type: ignore[operator]
                    return
            self._error("Not found", 404)
        except Exception as exc:
            web_server.log.exception("dev live-db GET %s failed", path)
            web_server._try_reconnect_db()
            self._error(str(exc), 500)

    def _serve_events(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        last_version = self.server.watch_version()
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                time.sleep(0.5)
                version = self.server.watch_version()
                if version != last_version:
                    last_version = version
                    self.wfile.write(b"data: reload\n\n")
                    self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            return

    def _dev_injection(self) -> str:
        label = html.escape(self.server.config.badge_text)
        return f"""
<style>
#cratedigger-dev-badge {{
  position: fixed;
  right: 10px;
  bottom: 10px;
  z-index: 99999;
  padding: 5px 8px;
  border: 1px solid #6a9;
  border-radius: 4px;
  background: #102018;
  color: #8dcbad;
  font: 11px/1.2 ui-monospace, SFMono-Regular, Menlo, monospace;
  opacity: 0.9;
  pointer-events: none;
}}
</style>
<div id="cratedigger-dev-badge">{label}</div>
<script>
(() => {{
  const events = new EventSource('/__dev/events');
  events.onmessage = event => {{
    if (event.data === 'reload') window.location.reload();
  }};
}})();
</script>
"""

    def _json(self, data: Any, status: int = 200) -> None:
        body = json.dumps(data).encode("utf-8")
        self._send_bytes(body, "application/json; charset=utf-8", status=status)

    def _error(self, msg: str, status: int = 400) -> None:
        self._json({"error": msg}, status=status)

    def _send_bytes(
        self,
        body: bytes,
        content_type: str,
        *,
        status: int = 200,
        cache_control: str | None = None,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if cache_control:
            self.send_header("Cache-Control", cache_control)
        self.end_headers()
        self.wfile.write(body)


def _api_fixture_slug(path: str) -> str:
    return path.strip("/").replace("/", "__")


def configure_live_db(config: DevConfig) -> None:
    if not config.dsn:
        raise SystemExit("--dsn or PIPELINE_DB_DSN is required for --data live-db")

    import web.server as web_server
    from lib.beets_db import BeetsDB
    from lib.pipeline_db import PipelineDB

    def connect_readonly() -> None:
        if web_server.db is not None:
            try:
                web_server.db.conn.close()
            except Exception:
                pass
        web_server.db = PipelineDB(config.dsn)
        web_server.db._execute("SET default_transaction_read_only = on")
        web_server.log.info("Connected dev live-db session in read-only mode")

    web_server._db_dsn = config.dsn
    connect_readonly()
    web_server._try_reconnect_db = connect_readonly  # type: ignore[assignment]

    web_server.beets_db_path = config.beets_db
    if config.beets_db and os.path.exists(config.beets_db):
        web_server._beets = BeetsDB(config.beets_db)
    else:
        web_server._beets = None

    if config.redis_host:
        web_server.cache.init(config.redis_host, config.redis_port)


def build_config(args: argparse.Namespace) -> DevConfig:
    return DevConfig(
        data=args.data,
        scenario=args.scenario,
        prod_base_url=args.prod_base_url,
        dsn=args.dsn,
        beets_db=args.beets_db,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
    )


def create_server(host: str, port: int, config: DevConfig) -> DevHTTPServer:
    if config.data == "live-db":
        configure_live_db(config)
    return DevHTTPServer((host, port), DevHandler, config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cratedigger frontend dev server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8096)
    parser.add_argument(
        "--data",
        choices=("fixture", "prod-api", "live-db"),
        default="fixture",
        help="API data source",
    )
    parser.add_argument(
        "--scenario",
        default="peer_dirs",
        help="Fixture scenario under tests/fixtures/web/",
    )
    parser.add_argument("--prod-base-url", default=PROD_BASE_URL)
    parser.add_argument("--dsn", default=os.environ.get("PIPELINE_DB_DSN"))
    parser.add_argument("--beets-db", default=os.environ.get("BEETS_DB_PATH"))
    parser.add_argument("--redis-host", default=None)
    parser.add_argument("--redis-port", type=int, default=6379)
    args = parser.parse_args()

    config = build_config(args)
    server = create_server(args.host, args.port, config)
    url = f"http://{args.host}:{args.port}"
    print(f"Cratedigger frontend dev server listening on {url}")
    print(f"  data: {config.data}")
    if config.data == "fixture":
        print(f"  scenario: {config.scenario}")
    if config.data == "prod-api":
        print(f"  proxy: {config.prod_base_url}")
    if config.data == "live-db":
        print("  live DB: read-only session")
    print("  mutating API requests: blocked")
    print("  live reload: web/index.html, web/js/*.js, active fixtures")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
