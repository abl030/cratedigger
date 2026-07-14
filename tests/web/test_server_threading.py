"""Threading contract for the web server (#427).

The production server is a ``ThreadingHTTPServer`` speaking HTTP/1.1
keep-alive, with per-thread pipeline/beets DB handles. These tests pin
the four load-bearing properties:

1. A slow request must not block other requests (the head-of-line
   blocking that made one wedged route a 9-hour outage in #233).
2. Keep-alive works: one client connection serves multiple requests,
   and bodyless responses (OPTIONS) declare ``Content-Length: 0``.
3. ``_db()`` hands each thread its own ``PipelineDB`` when a DSN is
   configured, while the injected-handle path (this very harness)
   keeps returning the shared object.
4. A client abort after becoming a metadata-flight leader cannot cancel
   the fill; the next HTTP request reads the completed cache entry.
"""
import configparser
import http.client
import os
import socket
import struct
import sys
import threading
import time
import tempfile
import unittest
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import conftest  # noqa: F401 — sets TEST_DB_DSN for the per-thread test

from tests.web._harness import _WebServerCase

TEST_DSN = os.environ.get("TEST_DB_DSN")


class TestConcurrentRequests(_WebServerCase):
    """A slow route must not block a concurrent fast one."""

    def test_slow_request_does_not_block_fast_request(self):
        from web import server as srv

        release_gate = threading.Event()
        entered_slow = threading.Event()

        def slow_route(h, params):
            entered_slow.set()
            # Block until the fast request has completed (or time out so
            # a regression fails the test instead of hanging it).
            release_gate.wait(timeout=10)
            h._json({"slow": True})

        slow_result: dict[str, object] = {}

        def run_slow():
            slow_result["resp"] = self._get("/api/_test_slow")[1]

        with patch.dict(srv.Handler._FUNC_GET_ROUTES,
                        {"/api/_test_slow": slow_route}):
            slow_thread = threading.Thread(target=run_slow, daemon=True)
            slow_thread.start()
            self.assertTrue(entered_slow.wait(timeout=5),
                            "slow route never started")

            # While the slow request is parked inside its handler, a
            # second request must complete. On a single-threaded server
            # this GET would deadlock (and the gate timeout would fail
            # the test).
            start = time.monotonic()
            status, body = self._get("/api/_index")
            elapsed = time.monotonic() - start
            self.assertEqual(status, 200)
            self.assertIsInstance(body, list)
            self.assertLess(elapsed, 5.0)
            self.assertFalse(release_gate.is_set())

            release_gate.set()
            slow_thread.join(timeout=5)
            self.assertEqual(slow_result["resp"], {"slow": True})

    def test_concurrent_cold_compare_requests_execute_one_metadata_fill(self):
        """The real threaded HTTP route shares one cold compare skeleton."""
        from tests.test_web_cache import FakeRedis
        from web import cache

        artist_id = "664c3e0e-42d8-48c1-b209-1efca19c0325"
        entered = threading.Event()
        release = threading.Event()
        results: list[tuple[int, dict]] = []
        saved_redis = cache._redis
        cache._redis = FakeRedis()

        def mb_releases(_artist_id: str) -> list[dict]:
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("compare fill was not released")
            return []

        try:
            with patch("web.server.mb_api") as mock_mb, patch(
                "web.routes.browse.discogs_api",
            ) as mock_discogs, patch(
                "web.server.get_library_artist", return_value=[],
            ):
                mock_mb.get_artist_release_groups.side_effect = mb_releases
                mock_mb.get_artist_name.return_value = "Test Artist"
                mock_discogs.get_artist_releases.return_value = []
                mock_discogs.get_artist_name.return_value = "Test Artist"

                path = (
                    "/api/artist/compare?name=Test%20Artist&"
                    f"mbid={artist_id}&discogs_id=3840"
                )
                first = threading.Thread(
                    target=lambda: results.append(self._get(path)), daemon=True,
                )
                second = threading.Thread(
                    target=lambda: results.append(self._get(path)), daemon=True,
                )
                first.start()
                self.assertTrue(entered.wait(timeout=5))
                second.start()
                # The first route remains parked in the metadata fill while
                # the second reaches the same cache key and becomes a waiter.
                time.sleep(0.05)
                release.set()
                first.join(timeout=5)
                second.join(timeout=5)

                self.assertFalse(first.is_alive())
                self.assertFalse(second.is_alive())
                self.assertEqual([status for status, _body in results], [200, 200])
                self.assertEqual(mock_mb.get_artist_release_groups.call_count, 1)
                self.assertEqual(mock_discogs.get_artist_releases.call_count, 1)
        finally:
            release.set()
            cache._redis = saved_redis

    def test_aborted_compare_leader_finishes_fill_for_next_http_request(self):
        """A real client RST cannot cancel its route's metadata fill."""
        from tests.test_web_cache import FakeRedis
        from web import cache
        from web import server as srv

        artist_id = "664c3e0e-42d8-48c1-b209-1efca19c0325"
        path = (
            "/api/artist/compare?name=Test%20Artist&"
            f"mbid={artist_id}&discogs_id=3840"
        )
        compare_key = f"meta:artist:compare:v8:{artist_id}:3840"
        entered = threading.Event()
        release = threading.Event()
        cache_written = threading.Event()
        disconnect_logged = threading.Event()
        saved_redis = cache._redis
        client: socket.socket | None = None

        class SignallingRedis(FakeRedis):
            def setex(self, key: str, ttl: int, value: str) -> None:
                super().setex(key, ttl, value)
                if key == compare_key:
                    cache_written.set()

        def mb_releases(_artist_id: str) -> list[dict]:
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("aborted leader fill was not released")
            return []

        def record_warning(message: str, *args: object, **_kwargs: object) -> None:
            if message.startswith("Client disconnect on GET"):
                disconnect_logged.set()

        cache._redis = SignallingRedis()
        try:
            with patch("web.server.mb_api") as mock_mb, patch(
                "web.routes.browse.discogs_api",
            ) as mock_discogs, patch(
                "web.server.get_library_artist", return_value=[],
            ), patch.object(
                srv.log, "warning", side_effect=record_warning,
            ):
                mock_mb.get_artist_release_groups.side_effect = mb_releases
                mock_mb.get_artist_name.return_value = "Test Artist"
                mock_discogs.get_artist_releases.return_value = []
                mock_discogs.get_artist_name.return_value = "Test Artist"

                client = socket.create_connection(
                    ("127.0.0.1", self.port), timeout=5,
                )
                client.sendall(
                    (
                        f"GET {path} HTTP/1.1\r\n"
                        f"Host: 127.0.0.1:{self.port}\r\n"
                        "Connection: close\r\n\r\n"
                    ).encode()
                )
                self.assertTrue(entered.wait(timeout=5))

                # Abort while the production route is the elected cache-fill
                # leader. SO_LINGER(0) emits a TCP RST, making the later body
                # write fail instead of allowing a buffered graceful close.
                client.setsockopt(
                    socket.SOL_SOCKET,
                    socket.SO_LINGER,
                    struct.pack("ii", 1, 0),
                )
                client.close()
                client = None
                release.set()

                self.assertTrue(
                    cache_written.wait(timeout=5),
                    "leader did not populate the compare cache after abort",
                )
                self.assertTrue(
                    disconnect_logged.wait(timeout=5),
                    "server did not observe the aborted response write",
                )

                status, body = self._get(path)
                self.assertEqual(status, 200)
                self.assertEqual(body["mb_artist"]["name"], "Test Artist")
                self.assertEqual(mock_mb.get_artist_release_groups.call_count, 1)
                self.assertEqual(mock_discogs.get_artist_releases.call_count, 1)
        finally:
            release.set()
            if client is not None:
                client.close()
            cache._redis = saved_redis


class TestKeepAlive(_WebServerCase):
    """HTTP/1.1 keep-alive: one connection, many requests."""

    def test_two_requests_reuse_one_connection(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request("GET", "/api/_index")
            r1 = conn.getresponse()
            body1 = r1.read()
            self.assertEqual(r1.status, 200)
            self.assertTrue(body1)
            # Same socket: a second request only works if the server
            # honoured keep-alive (it would have closed an HTTP/1.0
            # connection after the first response).
            conn.request("GET", "/api/_index")
            r2 = conn.getresponse()
            self.assertEqual(r2.status, 200)
            self.assertTrue(r2.read())
        finally:
            conn.close()

    def test_options_declares_zero_content_length(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request("OPTIONS", "/api/_index")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.getheader("Content-Length"), "0")
            resp.read()
        finally:
            conn.close()


class TestProductionWiringOverlays(unittest.TestCase):
    """With production wiring (`_db_dsn` set, `db` global None), the
    pipeline-status overlays must still work.

    Regression guard for the #427 P1: ``check_pipeline`` used to gate on
    the ``db`` global, which production no longer assigns — every
    browse/library row silently lost its pipeline badge while the
    injected-handle test harness kept passing."""

    def setUp(self):
        from web import server as srv
        self._srv = srv
        self._saved_dsn = srv._db_dsn
        self._saved_db = srv.db
        srv._db_dsn = TEST_DSN
        srv.db = None
        from tests.test_pipeline_db import make_db
        self._pg = make_db()  # truncates tables for an isolated slate

    def tearDown(self):
        self._pg.close()
        handle = getattr(self._srv._thread_state, "db", None)
        if handle is not None:
            handle.close()
            self._srv._thread_state.db = None
        self._srv._db_dsn = self._saved_dsn
        self._srv.db = self._saved_db

    def test_check_pipeline_finds_rows_without_db_global(self):
        rid = self._pg.add_request(
            artist_name="Wired", album_title="For Prod",
            source="request",
            mb_release_id="prod-wiring-mbid", status="wanted",
        )
        info = self._srv.check_pipeline(["prod-wiring-mbid"])
        self.assertIn("prod-wiring-mbid", info)
        self.assertEqual(info["prod-wiring-mbid"]["id"], rid)
        self.assertEqual(info["prod-wiring-mbid"]["status"], "wanted")



class TestPerThreadDbHandles(unittest.TestCase):
    """``_db()`` is thread-local with a DSN, shared when injected."""

    def setUp(self):
        from web import server as srv
        self._srv = srv
        self._saved_dsn = srv._db_dsn
        self._saved_db = srv.db

    def tearDown(self):
        self._srv._db_dsn = self._saved_dsn
        self._srv.db = self._saved_db

    def test_each_thread_gets_its_own_connection(self):
        srv = self._srv
        srv._db_dsn = TEST_DSN
        srv.db = None

        handles: dict[str, object] = {}

        def grab(label: str):
            handle = srv._db()
            # Same thread, same handle (cached in the thread-local).
            assert srv._db() is handle
            handles[label] = handle
            handle.close()

        t1 = threading.Thread(target=grab, args=("t1",))
        t2 = threading.Thread(target=grab, args=("t2",))
        t1.start(); t2.start()
        t1.join(timeout=10); t2.join(timeout=10)

        self.assertIn("t1", handles)
        self.assertIn("t2", handles)
        self.assertIsNot(handles["t1"], handles["t2"])

    def test_reconnect_drops_only_this_threads_handle(self):
        srv = self._srv
        srv._db_dsn = TEST_DSN
        srv.db = None

        first = srv._db()
        srv._try_reconnect_db()
        second = srv._db()
        try:
            self.assertIsNot(first, second)
        finally:
            second.close()
            # Drop the thread-local so later tests in this thread don't
            # inherit a closed handle.
            srv._thread_state.db = None

    def test_finish_closes_this_threads_handles_deterministically(self):
        """#435: connection teardown closes the thread-local psycopg2
        handle instead of leaving it to GC. Injected shared handles are
        out of scope (the dev server / harness own those)."""
        srv = self._srv
        srv._db_dsn = TEST_DSN
        srv.db = None

        handle = srv._db()
        self.assertFalse(handle.conn.closed)
        srv._close_thread_handles()
        self.assertTrue(handle.conn.closed)
        self.assertIsNone(getattr(srv._thread_state, "db", None))

    def test_close_thread_handles_leaves_injected_handle_alone(self):
        srv = self._srv
        srv._db_dsn = None
        sentinel = object()
        srv.db = sentinel  # type: ignore[assignment]
        srv._close_thread_handles()
        self.assertIs(srv.db, sentinel)

    def test_injected_handle_is_shared_across_threads(self):
        srv = self._srv
        srv._db_dsn = None
        sentinel = object()
        srv.db = sentinel  # type: ignore[assignment]

        seen: list[object] = []

        def grab():
            seen.append(srv._db())

        t = threading.Thread(target=grab)
        t.start(); t.join(timeout=10)
        self.assertEqual(seen, [sentinel])
        self.assertIs(srv._db(), sentinel)


class TestPerThreadBeetsHandles(unittest.TestCase):
    """Production Beets handles carry the configured library root."""

    def setUp(self):
        from web import server as srv
        self._srv = srv
        self._saved_path = srv.beets_db_path
        self._saved_root = srv.beets_library_root
        self._saved_injected = srv._beets
        self._saved_dsn = srv._db_dsn
        self._saved_mb_api_base = srv.mb_api.MB_API_BASE
        self._saved_discogs_api_base = srv._discogs.DISCOGS_API_BASE

    def tearDown(self):
        self._srv._close_thread_handles()
        self._srv.beets_db_path = self._saved_path
        self._srv.beets_library_root = self._saved_root
        self._srv._beets = self._saved_injected
        self._srv._db_dsn = self._saved_dsn
        self._srv.mb_api.MB_API_BASE = self._saved_mb_api_base
        self._srv._discogs.DISCOGS_API_BASE = self._saved_discogs_api_base

    def test_constructor_receives_library_root(self):
        from lib.config import CratediggerConfig

        srv = self._srv
        ini = configparser.ConfigParser()
        ini["Beets"] = {"directory": "/mnt/virtio/Music/Beets"}
        cfg = CratediggerConfig.from_ini(ini)
        with tempfile.NamedTemporaryFile() as db_file:
            srv.beets_db_path = db_file.name
            srv._beets = None

            with patch("lib.config.read_runtime_config", return_value=cfg):
                srv._configure_beets_library_root_from_runtime_config()
                handle = srv._beets_db()

            self.assertIsNotNone(handle)
            assert handle is not None
            self.assertEqual(
                handle._library_root,  # noqa: SLF001 - constructor seam
                "/mnt/virtio/Music/Beets",
            )

    def test_main_executes_runtime_root_wiring(self):
        """Production boot must load the root before opening the server."""
        from lib.config import CratediggerConfig

        class BootStop(Exception):
            pass

        srv = self._srv
        ini = configparser.ConfigParser()
        ini["Beets"] = {"directory": "/boot-config/Music/Beets"}
        cfg = CratediggerConfig.from_ini(ini)
        with tempfile.NamedTemporaryFile() as db_file, patch.object(
            sys,
            "argv",
            [
                "server.py",
                "--dsn",
                str(TEST_DSN),
                "--beets-db",
                db_file.name,
            ],
        ), patch(
            "lib.config.read_runtime_config",
            return_value=cfg,
        ), patch(
            "web.server.ThreadingHTTPServer",
            side_effect=BootStop,
        ):
            with self.assertRaises(BootStop):
                srv.main()

        self.assertEqual(srv.beets_library_root, "/boot-config/Music/Beets")


if __name__ == "__main__":
    unittest.main()
