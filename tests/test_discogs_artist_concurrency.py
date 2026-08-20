"""Real-HTTP concurrency and failure contracts for Discogs artist bulk reads."""
from __future__ import annotations

import concurrent.futures
import json
import threading
import time
import unittest
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from hypothesis import given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from web import discogs


class _DiscogsMirror:
    def __init__(
        self,
        *,
        delay: float = 0,
        fail_masters: bool = False,
        handler_exit_delay: float = 0,
    ) -> None:
        self.delay = delay
        self.fail_masters = fail_masters
        # Issue #1175. Stands in for the scheduling gap full-suite load opens
        # between a handler finishing its response and its thread being
        # scheduled again to tear down. The client has already read the body
        # and released its semaphore slot by then, so anything still counted
        # during this window is measuring handler lifetime rather than the
        # window the cap governs.
        self.handler_exit_delay = handler_exit_delay
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()
        mirror = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                masters = self.path.endswith("/masters/all")
                with mirror.lock:
                    mirror.active += 1
                    mirror.max_active = max(mirror.max_active, mirror.active)
                counted = True

                def leave() -> None:
                    # Closes the counted window at the last moment before this
                    # response becomes readable by the client. Everything after
                    # it -- writing bytes, this thread being descheduled, the
                    # handler returning -- happens while the client may already
                    # have read the body and released its semaphore slot, so
                    # counting it would measure handler lifetime instead of the
                    # window the cap governs (#1175). Idempotent, so the
                    # ``finally`` below still guarantees exactly one decrement
                    # on the paths that raise before reaching a response.
                    nonlocal counted
                    if counted:
                        counted = False
                        with mirror.lock:
                            mirror.active -= 1

                try:
                    if masters and mirror.fail_masters:
                        leave()
                        self.send_response(500)
                        self.end_headers()
                        return
                    if mirror.delay:
                        time.sleep(mirror.delay)
                    body = json.dumps({
                        "results": [], "total": 0, "page": 1, "per_page": 1,
                    }).encode()
                    leave()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                finally:
                    if mirror.handler_exit_delay:
                        time.sleep(mirror.handler_exit_delay)
                    leave()

            def log_message(self, format: str, *_args: object) -> None:
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever)

    def __enter__(self) -> str:
        self.thread.start()
        address = self.server.server_address
        return f"http://{address[0]}:{address[1]}"

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


def assert_discogs_cap(max_active: int) -> None:
    """The budget is read from the production constant, not re-typed here — a
    hand-written literal would pin a number rather than the invariant that the
    mirror's configured budget is honoured."""
    cap = discogs._DISCOGS_ARTIST_CONCURRENCY
    if max_active > cap:
        raise AssertionError(f"Discogs mirror cap exceeded: {max_active}")


class TestDiscogsArtistGlobalConcurrency(unittest.TestCase):
    def _run_calls(self, calls: int, *, handler_exit_delay: float = 0) -> _DiscogsMirror:
        mirror = _DiscogsMirror(delay=0.03, handler_exit_delay=handler_exit_delay)
        with mirror as base, patch.object(discogs, "DISCOGS_API_BASE", base), patch(
            "web.discogs._cache.memoize_meta", side_effect=lambda _key, fetch: fetch(),
        ), concurrent.futures.ThreadPoolExecutor(max_workers=calls) as executor:
            results = list(executor.map(discogs.get_artist_releases, range(1, calls + 1)))
        self.assertEqual(results, [[] for _ in range(calls)])
        return mirror

    def test_outbound_counter_records_each_mirror_http_attempt(self) -> None:
        attempts: list[str] = []
        mirror = _DiscogsMirror()
        with mirror as base, patch.object(discogs, "DISCOGS_API_BASE", base), patch.object(
            discogs, "_on_mirror_request", lambda: attempts.append("discogs"),
        ), patch("web.discogs._cache.memoize_meta", side_effect=lambda _key, fetch: fetch()):
            self.assertEqual(discogs.get_artist_releases(1), [])
        self.assertEqual(attempts, ["discogs", "discogs"])

    def test_three_top_level_artist_reads_share_one_two_request_budget(self) -> None:
        # Issue #1156 item 6: a prior round of this fix added
        # handler_exit_delay=0.02 here on the theory that this test shared
        # test_slow_handler_teardown_does_not_inflate_the_measured_window's
        # #1175 counted-window symptom. Independent review (third round)
        # traced _DiscogsMirror.do_GET and found that theory unsupported:
        # leave() closes the counted window BEFORE send_response and is
        # idempotent, so handler_exit_delay's sleep in the `finally` block
        # only delays handler-thread TEARDOWN (which ThreadingHTTPServer
        # does not serialize against) -- it cannot move mirror.max_active
        # either way. An A/B load probe on a 30-core host was inconclusive
        # (0/25 failures with the delay, 0/25 without), which is consistent
        # with the delay being a no-op here, not evidence either way of the
        # underlying flake reported in issue #1156 item 6. Left unchanged
        # rather than keeping an unsubstantiated fix; the demonstrated,
        # reproduced half of item 6 is the Node worker startup timeout in
        # tests/test_node_jsonl_worker.py.
        mirror = self._run_calls(3)
        # Saturation AND the cap in one assertion: with the budget honoured the
        # only value three concurrent readers can produce is the budget itself.
        # A degenerate counted window (reading 1) fails here just as a breached
        # cap does.
        self.assertEqual(mirror.max_active, discogs._DISCOGS_ARTIST_CONCURRENCY)

    def test_slow_handler_teardown_does_not_inflate_the_measured_window(self) -> None:
        """#1175: the counter must measure the window the client's semaphore
        hold governs, not handler lifetime.

        A handler descheduled after writing its response has already released
        the client's slot — the client read the body and left ``_get``'s
        ``with`` block. Counting that trailing window let a third reader be
        observed while the client-side cap of two was never breached, which is
        why this failed intermittently under full-suite load and passed in
        isolation.
        """
        mirror = self._run_calls(3, handler_exit_delay=0.02)
        assert_discogs_cap(mirror.max_active)

    @settings(max_examples=8)
    @given(calls=st.integers(min_value=1, max_value=5))
    def test_generated_simultaneous_artist_reads_never_exceed_global_cap(self, calls: int) -> None:
        assert_discogs_cap(self._run_calls(calls).max_active)

    def test_checker_rejects_the_per_invocation_cap_mutant(self) -> None:
        with self.assertRaisesRegex(AssertionError, "cap exceeded"):
            assert_discogs_cap(6)

    def test_one_fast_failure_does_not_wait_for_a_slow_sibling(self) -> None:
        mirror = _DiscogsMirror(delay=1, fail_masters=True)
        with mirror as base, patch.object(discogs, "DISCOGS_API_BASE", base), patch(
            "web.discogs._cache.memoize_meta", side_effect=lambda _key, fetch: fetch(),
        ):
            started = time.monotonic()
            with self.assertRaises(urllib.error.HTTPError) as raised:
                discogs.get_artist_releases(1)
            self.assertLess(time.monotonic() - started, 0.5)
        raised.exception.close()


if __name__ == "__main__":
    unittest.main()
