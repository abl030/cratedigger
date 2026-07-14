#!/usr/bin/env python3
"""Tests for scripts/web_dev_server.py."""

from __future__ import annotations

import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from unittest.mock import patch

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 — bootstraps TEST_DB_DSN for the live-db test
from tests.test_web_cache import FakeRedis

from scripts.web_dev_server import (
    DevConfig,
    DevHandler,
    DevHTTPServer,
    create_server,
)


class WebDevServerTest(unittest.TestCase):
    def setUp(self) -> None:
        config = DevConfig(
            data="fixture",
            scenario="peers",
            prod_base_url="https://music.ablz.au",
            dsn=None,
            beets_db=None,
            mb_api=None,
            discogs_api=None,
            redis_host=None,
            redis_port=6379,
        )
        self.server = DevHTTPServer(("127.0.0.1", 0), DevHandler, config)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def get_json(self, path: str) -> dict:
        with urlopen(f"{self.base}{path}") as resp:
            self.assertEqual(resp.status, 200)
            return json.loads(resp.read())

    def test_serves_index_with_dev_badge_and_reload_hook(self):
        with urlopen(f"{self.base}/") as resp:
            body = resp.read().decode()

        self.assertIn("DEV fixture:peers", body)
        self.assertIn("new EventSource('/__dev/events')", body)
        self.assertIn('type="module" src="/js/main.js"', body)

    def test_serves_fixture_api_scenario(self):
        payload = self.get_json("/api/pipeline/dashboard")

        self.assertEqual(payload["peers"]["totals"]["known_peers"], 316)
        self.assertEqual(payload["peers"]["days"][0]["new_peers"], 316)

    def test_unknown_fixture_route_is_a_404_json(self):
        with self.assertRaises(HTTPError) as raised:
            urlopen(f"{self.base}/api/not-real")

        self.assertEqual(raised.exception.code, 404)
        payload = json.loads(raised.exception.read())
        self.assertEqual(payload["path"], "/api/not-real")

    def test_mutating_api_requests_are_blocked(self):
        req = Request(
            f"{self.base}/api/pipeline/delete",
            data=b'{"id":1}',
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with self.assertRaises(HTTPError) as raised:
            urlopen(req)

        self.assertEqual(raised.exception.code, 405)
        payload = json.loads(raised.exception.read())
        self.assertIn("blocked", payload["error"])


class _FakeUpstreamResponse:
    def __init__(self, body: bytes, *, status: int, headers: dict[str, str]):
        self._body = body
        self.status = status
        self.headers = headers

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class WebDevServerProxyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.captured_request = None
        config = DevConfig(
            data="prod-api",
            scenario="peers",
            prod_base_url="http://upstream.test",
            dsn=None,
            beets_db=None,
            mb_api=None,
            discogs_api=None,
            redis_host=None,
            redis_port=6379,
        )
        self.urlopen_patch = patch(
            "scripts.web_dev_server.urllib.request.urlopen",
            side_effect=self._fake_urlopen,
        )
        self.urlopen_patch.start()
        self.server = DevHTTPServer(("127.0.0.1", 0), DevHandler, config)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.urlopen_patch.stop()

    def _fake_urlopen(self, req, timeout=30):
        self.captured_request = req
        return _FakeUpstreamResponse(
            b"bcd",
            status=206,
            headers={
                "Content-Type": "audio/mpeg",
                "Content-Range": "bytes 1-3/6",
                "Accept-Ranges": "bytes",
                "Content-Length": "3",
            },
        )

    def test_prod_api_proxy_forwards_range_headers(self):
        req = Request(
            f"{self.base}/api/wrong-matches/audio?download_log_id=42&path=01.mp3",
            headers={"Range": "bytes=1-3"},
        )
        with urlopen(req) as resp:
            body = resp.read()

        assert self.captured_request is not None
        self.assertEqual(self.captured_request.full_url, "http://upstream.test/api/wrong-matches/audio?download_log_id=42&path=01.mp3")
        self.assertEqual(self.captured_request.headers.get("Range"), "bytes=1-3")
        self.assertEqual(resp.status, 206)
        self.assertEqual(resp.headers.get("Content-Range"), "bytes 1-3/6")
        self.assertEqual(resp.headers.get("Accept-Ranges"), "bytes")
        self.assertEqual(body, b"bcd")


class WebDevServerLiveDbErrorMappingTest(unittest.TestCase):
    """#501 item 4: the `--data live-db` GET dispatch's generic
    `except Exception` mapped EVERY route-handler exception to 500,
    including `DiscogsMirrorNotConfigured` — which production's
    `web/server.py::do_GET` maps to 503 (a deliberate config posture, not
    a crash). Dev sessions should exercise the same status code."""

    def setUp(self) -> None:
        config = DevConfig(
            data="live-db",
            scenario="peers",
            prod_base_url="https://music.ablz.au",
            dsn=None,
            beets_db=None,
            mb_api=None,
            discogs_api=None,
            redis_host=None,
            redis_port=6379,
        )
        self.server = DevHTTPServer(("127.0.0.1", 0), DevHandler, config)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def test_discogs_mirror_not_configured_maps_to_503_not_500(self):
        import web.server as web_server
        from web.discogs import DiscogsMirrorNotConfigured

        def _raise(h, params):
            raise DiscogsMirrorNotConfigured("no mirror configured")

        # Throwaway route registration (test-only wiring into the real
        # route dict, not a mock of our own logic) so the real exception
        # class flows through the real dispatch path (test-fidelity Rule B).
        probe_path = "/api/__test_mirror_probe"
        web_server.Handler._FUNC_GET_ROUTES[probe_path] = _raise
        self.addCleanup(
            web_server.Handler._FUNC_GET_ROUTES.pop, probe_path, None,
        )

        with self.assertRaises(HTTPError) as raised:
            urlopen(f"{self.base}{probe_path}")
        self.assertEqual(raised.exception.code, 503)


class _MetadataMirrorServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, kind: str):
        super().__init__(("127.0.0.1", 0), _MetadataMirrorHandler)
        self.kind = kind
        self.requests: list[str] = []

    @property
    def origin(self) -> str:
        return f"http://127.0.0.1:{self.server_port}"


class _MetadataMirrorHandler(BaseHTTPRequestHandler):
    server: _MetadataMirrorServer  # pyright: ignore[reportIncompatibleVariableOverride]

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002
        return

    def do_GET(self) -> None:
        self.server.requests.append(self.path)
        parsed = urlparse(self.path)
        if self.server.kind == "mb":
            if parsed.path == "/ws/2/release-group":
                payload = {"release-groups": [], "release-group-count": 0}
            elif parsed.path == "/ws/2/release":
                payload = {"releases": [], "release-count": 0}
            elif parsed.path == "/ws/2/artist/test-mbid":
                payload = {"id": "test-mbid", "name": "Synthetic Artist"}
            else:
                self.send_error(404)
                return
        elif parsed.path in (
            "/api/artists/60/masters/all",
            "/api/artists/60/appearances",
        ):
            payload = {
                "results": [], "total": 0, "page": 1, "per_page": 100,
            }
        elif parsed.path == "/api/artists/60":
            payload = {"id": 60, "name": "Synthetic Artist"}
        elif parsed.path == "/api/releases/60":
            payload = {
                "id": 60,
                "title": "Synthetic Release",
                "country": "",
                "released": "",
                "master_id": None,
                "artists": [{"id": 60, "name": "Synthetic Artist"}],
                "labels": [],
                "formats": [],
                "tracks": [],
            }
        else:
            self.send_error(404)
            return

        body = json.dumps(payload).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class WebDevServerLiveDbMetadataIntegrationTest(unittest.TestCase):
    """The real live-db compare route must use its configured mirrors."""

    def setUp(self) -> None:
        import web.discogs
        import web.cache
        import web.mb
        import web.server

        self.dsn = os.environ.get("TEST_DB_DSN")
        self.web_discogs = web.discogs
        self.web_cache = web.cache
        self.web_mb = web.mb
        self.web_server = web.server
        self.saved_redis = web.cache._redis
        self.metadata_cache = FakeRedis()
        web.cache._redis = self.metadata_cache
        self.saved_metadata = (
            web.mb.MB_API_BASE,
            web.discogs.DISCOGS_API_BASE,
        )
        self.saved_server = (
            web.server._db_dsn,
            web.server.db,
            web.server._try_reconnect_db,
            web.server.beets_db_path,
            web.server._beets,
        )
        self.running: list[tuple[ThreadingHTTPServer, threading.Thread]] = []

    def tearDown(self) -> None:
        for server, thread in reversed(self.running):
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        if (
            self.web_server.db is not None
            and self.web_server.db is not self.saved_server[1]
        ):
            self.web_server.db.close()
        (
            self.web_server._db_dsn,
            self.web_server.db,
            self.web_server._try_reconnect_db,
            self.web_server.beets_db_path,
            self.web_server._beets,
        ) = self.saved_server
        (
            self.web_mb.MB_API_BASE,
            self.web_discogs.DISCOGS_API_BASE,
        ) = self.saved_metadata
        self.web_cache._redis = self.saved_redis

    def _start(self, server: ThreadingHTTPServer) -> threading.Thread:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.running.append((server, thread))
        return thread

    def _live_config(
        self, *, mb_api: str | None, discogs_api: str | None,
    ) -> DevConfig:
        return DevConfig(
            data="live-db",
            scenario="peers",
            prod_base_url="https://music.ablz.au",
            dsn=self.dsn,
            beets_db=None,
            mb_api=mb_api,
            discogs_api=discogs_api,
            redis_host=None,
            redis_port=6379,
        )

    def _start_live_server(self, config: DevConfig) -> str:
        server = create_server("127.0.0.1", 0, config)
        self._start(server)
        return f"http://127.0.0.1:{server.server_port}"

    def test_configured_compare_reaches_both_origins_then_missing_discogs_503s(
        self,
    ) -> None:
        mb = _MetadataMirrorServer("mb")
        discogs = _MetadataMirrorServer("discogs")
        self._start(mb)
        self._start(discogs)

        base = self._start_live_server(self._live_config(
            mb_api=f"{mb.origin}/ws/2",
            discogs_api=discogs.origin,
        ))
        path = (
            "/api/artist/compare?name=Synthetic%20Artist&"
            "mbid=test-mbid&discogs_id=60"
        )
        with urlopen(f"{base}{path}") as response:
            payload = json.loads(response.read())

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["mb_artist"]["name"], "Synthetic Artist")
        self.assertEqual(payload["discogs_artist"]["name"], "Synthetic Artist")
        self.assertTrue(any(
            request.startswith("/ws/2/release-group?artist=test-mbid&")
            for request in mb.requests
        ), mb.requests)
        self.assertTrue(any(
            request.startswith("/ws/2/release?track_artist=test-mbid&")
            for request in mb.requests
        ), mb.requests)
        self.assertTrue(any(
            request.startswith(
                "/ws/2/release?artist=test-mbid&inc=release-groups&"
            )
            for request in mb.requests
        ), mb.requests)
        self.assertIn("/ws/2/artist/test-mbid?fmt=json", mb.requests)
        self.assertIn("/api/artists/60/masters/all", discogs.requests)
        self.assertIn("/api/artists/60/appearances", discogs.requests)
        self.assertIn("/api/artists/60", discogs.requests)
        self.assertTrue({
            "meta:artist:compare:v8:test-mbid:60",
            "meta:mb:artist:test-mbid:name",
            "meta:discogs:artist:60:name",
        }.issubset(self.metadata_cache._store))

        resolve_path = "/api/browse/resolve?id=60&source=discogs&kind=release"
        with urlopen(f"{base}{resolve_path}") as resolve_response:
            resolve_payload = json.loads(resolve_response.read())
        self.assertEqual(resolve_response.status, 200)
        self.assertEqual(resolve_payload["source"], "discogs")
        self.assertIn(
            resolve_payload["target_identity_kind"], {"work", "release"},
        )
        self.assertTrue({
            "meta:discogs:release:v2:60",
            "meta:browse-resolve:v2:discogs:release:60",
        }.issubset(self.metadata_cache._store))

        # A second live-db configuration in the same process must clear the
        # first session's Discogs origin and reject BEFORE reading the fully
        # warm compare/name cache. Otherwise screenshot QA can false-green
        # after an earlier configured server populated both process surfaces.
        missing_base = self._start_live_server(self._live_config(
            mb_api=f"{mb.origin}/ws/2",
            discogs_api=None,
        ))
        with self.assertRaises(HTTPError) as raised:
            urlopen(f"{missing_base}{path}")
        self.assertEqual(raised.exception.code, 503)
        with self.assertRaises(HTTPError) as resolve_raised:
            urlopen(f"{missing_base}{resolve_path}")
        self.assertEqual(resolve_raised.exception.code, 503)


class ConfigureLiveDbReadOnlyTest(unittest.TestCase):
    """`--data live-db` sessions must stay read-only after #427.

    `_db()` opens fresh per-thread connections whenever `_db_dsn` is
    set, skipping the `SET default_transaction_read_only = on` that
    configure_live_db applies to its injected handle. The guard here is
    that configure_live_db leaves `_db_dsn` unset, so every request
    routes through the single read-only handle — and a write through
    `_db()` is rejected by PostgreSQL."""

    def setUp(self) -> None:
        import os
        self.dsn = os.environ.get("TEST_DB_DSN")
        import web.server as web_server
        self.web_server = web_server
        self._saved = (
            web_server._db_dsn, web_server.db, web_server._try_reconnect_db,
            web_server.beets_db_path, web_server._beets,
        )

    def tearDown(self) -> None:
        ws = self.web_server
        if ws.db is not None and ws.db is not self._saved[1]:
            try:
                ws.db.close()
            except Exception:
                pass
        (ws._db_dsn, ws.db, ws._try_reconnect_db,
         ws.beets_db_path, ws._beets) = self._saved

    def test_live_db_session_rejects_writes_through_db_accessor(self):
        import psycopg2
        from scripts.web_dev_server import configure_live_db

        config = DevConfig(
            data="live-db",
            scenario="peers",
            prod_base_url="https://music.ablz.au",
            dsn=self.dsn,
            beets_db=None,
            mb_api=None,
            discogs_api=None,
            redis_host=None,
            redis_port=6379,
        )
        configure_live_db(config)
        ws = self.web_server

        # _db_dsn must stay unset so _db() returns the injected
        # read-only handle instead of opening per-thread connections.
        self.assertEqual(ws._db_dsn, self._saved[0])
        self.assertIs(ws._db(), ws.db)

        with self.assertRaises(psycopg2.Error):
            ws._db()._execute(
                "INSERT INTO album_requests (artist_name, album_title, source)"
                " VALUES ('ro', 'ro', 'request')"
            )


if __name__ == "__main__":
    unittest.main()
