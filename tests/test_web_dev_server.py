#!/usr/bin/env python3
"""Tests for scripts/web_dev_server.py."""

from __future__ import annotations

import json
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from scripts.web_dev_server import DevConfig, DevHandler, DevHTTPServer


class WebDevServerTest(unittest.TestCase):
    def setUp(self) -> None:
        config = DevConfig(
            data="fixture",
            scenario="peer_dirs",
            prod_base_url="https://music.ablz.au",
            dsn=None,
            beets_db=None,
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

        self.assertIn("DEV fixture:peer_dirs", body)
        self.assertIn("new EventSource('/__dev/events')", body)
        self.assertIn('type="module" src="/js/main.js"', body)

    def test_serves_fixture_api_scenario(self):
        payload = self.get_json("/api/pipeline/dashboard")

        self.assertEqual(payload["peer_dirs"]["totals"]["known_combos"], 1621)
        self.assertEqual(payload["peer_dirs"]["days"][0]["new_peers"], 316)

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


if __name__ == "__main__":
    unittest.main()
