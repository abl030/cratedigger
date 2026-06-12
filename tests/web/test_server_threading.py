"""Threading contract for the web server (#427).

The production server is a ``ThreadingHTTPServer`` speaking HTTP/1.1
keep-alive, with per-thread pipeline/beets DB handles. These tests pin
the three load-bearing properties:

1. A slow request must not block other requests (the head-of-line
   blocking that made one wedged route a 9-hour outage in #233).
2. Keep-alive works: one client connection serves multiple requests,
   and bodyless responses (OPTIONS) declare ``Content-Length: 0``.
3. ``_db()`` hands each thread its own ``PipelineDB`` when a DSN is
   configured, while the injected-handle path (this very harness)
   keeps returning the shared object.
"""
import http.client
import os
import sys
import threading
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
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
            status, body = self._get("/api/pipeline/constants")
            elapsed = time.monotonic() - start
            self.assertEqual(status, 200)
            self.assertIsInstance(body, dict)
            self.assertLess(elapsed, 5.0)
            self.assertFalse(release_gate.is_set())

            release_gate.set()
            slow_thread.join(timeout=5)
            self.assertEqual(slow_result["resp"], {"slow": True})


class TestKeepAlive(_WebServerCase):
    """HTTP/1.1 keep-alive: one connection, many requests."""

    def test_two_requests_reuse_one_connection(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request("GET", "/api/pipeline/constants")
            r1 = conn.getresponse()
            body1 = r1.read()
            self.assertEqual(r1.status, 200)
            self.assertTrue(body1)
            # Same socket: a second request only works if the server
            # honoured keep-alive (it would have closed an HTTP/1.0
            # connection after the first response).
            conn.request("GET", "/api/pipeline/constants")
            r2 = conn.getresponse()
            self.assertEqual(r2.status, 200)
            self.assertTrue(r2.read())
        finally:
            conn.close()

    def test_options_declares_zero_content_length(self):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request("OPTIONS", "/api/pipeline/constants")
            resp = conn.getresponse()
            self.assertEqual(resp.status, 200)
            self.assertEqual(resp.getheader("Content-Length"), "0")
            resp.read()
        finally:
            conn.close()


class TestProductionWiringOverlays(unittest.TestCase):
    """With production wiring (`_db_dsn` set, `db` global None), the
    pipeline-status overlays must still work.

    Regression guard for the #427 P1: ``check_pipeline`` /
    ``_enrich_with_pipeline`` used to gate on the ``db`` global, which
    production no longer assigns — every browse/library row silently
    lost its pipeline badge while the injected-handle test harness kept
    passing."""

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

    def test_enrich_with_pipeline_mutates_albums_without_db_global(self):
        rid = self._pg.add_request(
            artist_name="Wired", album_title="For Prod",
            source="request",
            mb_release_id="prod-wiring-mbid", status="wanted",
        )
        self._pg.update_request_fields(
            rid, search_filetype_override="lossless")
        albums: list[dict[str, object]] = [
            {"mb_albumid": "prod-wiring-mbid"},
            {"mb_albumid": "not-in-pipeline"},
        ]
        self._srv._enrich_with_pipeline(albums)
        self.assertTrue(albums[0].get("upgrade_queued"))
        self.assertNotIn("upgrade_queued", albums[1])


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


if __name__ == "__main__":
    unittest.main()
