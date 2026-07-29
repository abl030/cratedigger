"""Focused checks for the cold artist benchmark harness.

Run: ``nix-shell --run "python3 -m unittest tests.test_bench_artist_cold -v"``.
"""
from __future__ import annotations

import concurrent.futures
import json
import subprocess
import threading
import unittest
import urllib.error
import urllib.request

from scripts.bench_artist_cold import (
    Artist,
    _BrowserMeasurementWire,
    _finish_mirror_measurement,
    _start_mirror_measurement,
    _stop_playwright_process,
    measure_browser,
    require_complete_browser_measurement,
)
from scripts.web_dev_server import (
    DevConfig,
    DevHandler,
    DevHTTPServer,
    MirrorRequestCounts,
    create_server,
)


class _MirrorLeaseServer:
    def __init__(self, *, data: str = "live-db", redis_host: str | None = None) -> None:
        self.data = data
        self.redis_host = redis_host

    def __enter__(self) -> str:
        config = DevConfig(
            data=self.data, scenario="default", prod_base_url="", dsn=None,
            beets_db=None, mb_api=None, discogs_api=None, redis_host=self.redis_host,
            redis_port=6379, measure_mirror_requests=True,
        )
        self.server = DevHTTPServer(("127.0.0.1", 0), DevHandler, config)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        return f"http://{self.server.server_address[0]}:{self.server.server_address[1]}"

    def __exit__(self, *_exc: object) -> None:
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


def _request(base: str, path: str, *, method: str = "GET") -> tuple[int, dict[str, object]]:
    try:
        with urllib.request.urlopen(urllib.request.Request(f"{base}{path}", method=method)) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


class _HungProcess:
    def __init__(self) -> None:
        self.terminated = 0
        self.killed = 0
        self.waits: list[int] = []

    def terminate(self) -> None:
        self.terminated += 1

    def kill(self) -> None:
        self.killed += 1

    def wait(self, timeout: float | None = None) -> int:
        assert timeout is not None
        self.waits.append(int(timeout))
        if len(self.waits) == 1:
            raise subprocess.TimeoutExpired("playwright-mcp", timeout)
        return 0


class TestBrowserBenchmarkChecks(unittest.TestCase):
    def test_completion_and_hung_process_contracts(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "compare or disambiguation"):
            require_complete_browser_measurement(_BrowserMeasurementWire(0, 0, 0, 0, 0, False))
        require_complete_browser_measurement(_BrowserMeasurementWire(0, 0, 0, 0, 0, True))
        process = _HungProcess()
        _stop_playwright_process(process)
        self.assertEqual((process.terminated, process.killed, process.waits), (1, 1, [10, 10]))

    def test_missing_command_releases_lease(self) -> None:
        with _MirrorLeaseServer() as base:
            with self.assertRaises(FileNotFoundError):
                measure_browser(base, Artist("missing", "missing"), "/definitely/not/a/command", mirror_counts=True)
            token = _start_mirror_measurement(base)
            self.assertEqual(_finish_mirror_measurement(base, token), (0, 0))

    def test_counter_counts_only_active_lease(self) -> None:
        counts = MirrorRequestCounts()
        token = counts.start()
        counts.record_mb(); counts.record_mb(); counts.record_discogs()
        self.assertEqual(counts.finish(token), {"mb_mirror_requests": 2, "discogs_mirror_requests": 1})
        counts.record_mb()
        next_token = counts.start()
        self.assertEqual(counts.finish(next_token), {"mb_mirror_requests": 0, "discogs_mirror_requests": 0})

    def test_only_uncached_live_db_is_accepted(self) -> None:
        for server in (_MirrorLeaseServer(data="prod-api"), _MirrorLeaseServer(redis_host="127.0.0.1")):
            with self.subTest(server=server.data, redis=server.redis_host), server as base, \
                    self.assertRaisesRegex(RuntimeError, "live-db dev server with Redis disabled"):
                _start_mirror_measurement(base)
        for data, redis_host, message in (("prod-api", None, "requires --data live-db"), ("live-db", "127.0.0.1", "requires Redis to be disabled")):
            config = DevConfig(data=data, scenario="default", prod_base_url="", dsn=None, beets_db=None, mb_api=None, discogs_api=None, redis_host=redis_host, redis_port=6379, measure_mirror_requests=True)
            with self.subTest(config=data), self.assertRaisesRegex(ValueError, message):
                create_server("127.0.0.1", 0, config)

    def test_real_http_lease_ownership_and_nonmixing(self) -> None:
        with _MirrorLeaseServer() as base:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                starts = list(executor.map(lambda _index: _request(base, "/__dev/mirror-counts/start", method="POST"), range(2)))
            successes = [payload for status, payload in starts if status == 200]
            self.assertEqual(len(successes), 1)
            self.assertEqual([status for status, _payload in starts].count(409), 1)
            token = successes[0]["token"]
            assert isinstance(token, str)
            self.assertEqual(_request(base, "/__dev/mirror-counts?token=not-owner")[0], 403)
            self.assertEqual(_request(base, f"/__dev/mirror-counts/finish?token={token}", method="POST"), (200, {"mb_mirror_requests": 0, "discogs_mirror_requests": 0}))
            status, next_payload = _request(base, "/__dev/mirror-counts/start", method="POST")
            self.assertEqual(status, 200)
            next_token = next_payload["token"]
            assert isinstance(next_token, str)
            self.assertEqual(_request(base, f"/__dev/mirror-counts/finish?token={next_token}", method="POST"), (200, {"mb_mirror_requests": 0, "discogs_mirror_requests": 0}))


if __name__ == "__main__":
    unittest.main()
