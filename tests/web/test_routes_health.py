"""Contract tests for the dependency-free liveness route handler."""

from __future__ import annotations

import unittest

from web.routes.health import serve_healthz


class _RecordingHandler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object | None]] = []
        self.close_connection = False

    def send_response_only(self, code: int) -> None:
        self.calls.append(("send_response_only", code))

    def send_header(self, keyword: str, value: str) -> None:
        self.calls.append(("send_header", (keyword, value)))

    def end_headers(self) -> None:
        self.calls.append(("end_headers", None))


class TestHealthRouteHandler(unittest.TestCase):
    def test_response_is_exact_bare_204(self) -> None:
        handler = _RecordingHandler()
        serve_healthz(handler)
        self.assertTrue(handler.close_connection)
        self.assertEqual(
            handler.calls,
            [
                ("send_response_only", 204),
                ("end_headers", None),
            ],
        )


if __name__ == "__main__":
    unittest.main()
