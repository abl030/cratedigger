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
    def __init__(self, *, delay: float = 0, fail_masters: bool = False) -> None:
        self.delay = delay
        self.fail_masters = fail_masters
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
                try:
                    if masters and mirror.fail_masters:
                        self.send_response(500)
                        self.end_headers()
                        return
                    if mirror.delay:
                        time.sleep(mirror.delay)
                    body = json.dumps({
                        "results": [], "total": 0, "page": 1, "per_page": 1,
                    }).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                finally:
                    with mirror.lock:
                        mirror.active -= 1

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
    if max_active > 2:
        raise AssertionError(f"Discogs mirror cap exceeded: {max_active}")


class TestDiscogsArtistGlobalConcurrency(unittest.TestCase):
    def _run_calls(self, calls: int) -> _DiscogsMirror:
        mirror = _DiscogsMirror(delay=0.03)
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
        mirror = self._run_calls(3)
        assert_discogs_cap(mirror.max_active)
        self.assertGreater(mirror.max_active, 1)

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
