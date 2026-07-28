"""Application request-security boundary tests."""

from __future__ import annotations

import http.client
import json
import socket
import threading
import unittest
from http.server import ThreadingHTTPServer
from typing import override
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from tests.web._harness import _WebServerCase
from web.request_security import (
    BROWSER_CHANNEL,
    CHANNEL_HEADER,
    CLI_CHANNEL,
    SAFE_METHODS,
    RequestSecurityError,
    authorize_request,
    is_exact_liveness_request,
)

CANONICAL_ORIGIN = "https://music.ablz.au"


def _allowed(
    *,
    method: str,
    channels: tuple[str, ...],
    origins: tuple[str, ...] = (),
    referers: tuple[str, ...] = (),
    canonical_origin: str = CANONICAL_ORIGIN,
) -> bool:
    try:
        authorize_request(
            method=method,
            channel_values=channels,
            origin_values=origins,
            referer_values=referers,
            canonical_origin=canonical_origin,
        )
    except RequestSecurityError:
        return False
    return True


class TestPureRequestSecurity(unittest.TestCase):
    def test_only_exact_browser_and_cli_channels_are_recognized(self) -> None:
        cases = (
            ("browser GET", "GET", (BROWSER_CHANNEL,), True),
            ("cli POST", "POST", (CLI_CHANNEL,), True),
            ("missing", "GET", (), False),
            ("unknown", "GET", ("trusted",), False),
            ("case variant", "GET", ("Browser",), False),
            ("duplicate", "GET", (BROWSER_CHANNEL, BROWSER_CHANNEL), False),
            ("serialized duplicate", "GET", ("browser, browser",), False),
        )
        for label, method, channels, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(
                    _allowed(method=method, channels=channels),
                    expected,
                )

    def test_browser_unsafe_methods_require_every_signal_to_match(self) -> None:
        cases = (
            ("exact Origin", ("https://music.ablz.au",), (), True),
            ("host case", ("https://MUSIC.ABLZ.AU",), (), True),
            ("explicit default port", ("https://music.ablz.au:443",), (), True),
            (
                "Referer fallback",
                (),
                ("https://music.ablz.au/library?tab=owned",),
                True,
            ),
            (
                "matching both",
                ("https://music.ablz.au",),
                ("https://music.ablz.au/#tab",),
                True,
            ),
            ("missing", (), (), False),
            ("null", ("null",), (), False),
            ("malformed", ("https://",), (), False),
            ("empty query delimiter", ("https://music.ablz.au?",), (), False),
            ("empty fragment delimiter", ("https://music.ablz.au#",), (), False),
            ("wrong scheme", ("http://music.ablz.au",), (), False),
            ("wrong host", ("https://evil.example",), (), False),
            ("prefix host", ("https://music.ablz.au.evil.example",), (), False),
            ("suffix host", ("https://evilmusic.ablz.au",), (), False),
            ("wrong port", ("https://music.ablz.au:444",), (), False),
            ("credentials", ("https://operator@music.ablz.au",), (), False),
            (
                "duplicate Origin fields",
                ("https://music.ablz.au", "https://music.ablz.au"),
                (),
                False,
            ),
            (
                "serialized Origin list",
                ("https://music.ablz.au https://evil.example",),
                (),
                False,
            ),
            (
                "serialized Referer list",
                (),
                ("https://music.ablz.au/path,https://evil.example/path",),
                False,
            ),
            (
                "conflicting signals",
                ("https://music.ablz.au",),
                ("https://evil.example/path",),
                False,
            ),
        )
        for label, origins, referers, expected in cases:
            with self.subTest(label=label):
                self.assertEqual(
                    _allowed(
                        method="POST",
                        channels=(BROWSER_CHANNEL,),
                        origins=origins,
                        referers=referers,
                    ),
                    expected,
                )

    def test_default_ports_normalize_for_both_http_schemes(self) -> None:
        cases = (
            ("http omitted to explicit", "http://example.test", "http://example.test:80"),
            ("http explicit to omitted", "http://example.test:80", "http://example.test"),
            ("https omitted to explicit", "https://example.test", "https://example.test:443"),
            ("https explicit to omitted", "https://example.test:443", "https://example.test"),
        )
        for label, canonical, supplied in cases:
            with self.subTest(label=label):
                self.assertTrue(
                    _allowed(
                        method="POST",
                        channels=(BROWSER_CHANNEL,),
                        origins=(supplied,),
                        canonical_origin=canonical,
                    )
                )

    def test_configured_origin_must_itself_be_one_exact_origin(self) -> None:
        values = (
            "",
            "null",
            "ftp://music.example",
            "https://operator@music.example",
            "https://music.example:",
            "https://music.example/",
            "https://music.example/path",
            "https://music.example?query=1",
            "https://music.example?",
            "https://music.example#fragment",
            "https://music.example#",
            " https://music.example",
        )
        for canonical in values:
            with self.subTest(canonical=canonical):
                self.assertFalse(
                    _allowed(
                        method="POST",
                        channels=(BROWSER_CHANNEL,),
                        origins=("https://music.example",),
                        canonical_origin=canonical,
                    )
                )

    def test_cli_does_not_require_browser_provenance(self) -> None:
        for method in ("POST", "PUT", "PATCH", "DELETE", "PURGE"):
            with self.subTest(method=method):
                self.assertTrue(_allowed(method=method, channels=(CLI_CHANNEL,)))

    def test_safe_browser_methods_do_not_require_provenance(self) -> None:
        self.assertEqual(SAFE_METHODS, frozenset({"GET", "HEAD", "OPTIONS"}))
        for method in SAFE_METHODS:
            with self.subTest(method=method):
                self.assertTrue(
                    _allowed(method=method, channels=(BROWSER_CHANNEL,))
                )

    def test_liveness_exception_is_exact(self) -> None:
        cases = (
            ("GET", "/healthz", True),
            ("HEAD", "/healthz", True),
            ("POST", "/healthz", False),
            ("GET", "/healthz?", False),
            ("GET", "/healthz?probe=1", False),
            ("GET", "//healthz", False),
            ("GET", "/healthz/", False),
            ("GET", "/x/../healthz", False),
            ("GET", "/health%7a", False),
            ("GET", "https://music.ablz.au/healthz", False),
        )
        for method, target, expected in cases:
            with self.subTest(method=method, target=target):
                self.assertEqual(
                    is_exact_liveness_request(method, target),
                    expected,
                )


class TestRequestSecurityHTTP(_WebServerCase):
    def _raw_request(
        self,
        method: str,
        target: str,
        *,
        headers: dict[str, str] | None = None,
        body: bytes | None = None,
    ) -> tuple[int, bytes, http.client.HTTPMessage]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            conn.request(method, target, body=body, headers=headers or {})
            response = conn.getresponse()
            payload = response.read()
            return response.status, payload, response.headers
        finally:
            conn.close()

    def test_rejected_browser_post_stops_before_body_route_or_service(self) -> None:
        from web import server as srv

        route_calls: list[dict[str, object]] = []

        def service_route(handler, body: dict[str, object]) -> None:
            route_calls.append(body)
            handler._json({"status": "unexpected"})

        class OrderingHandler(srv.Handler):
            body_reads = 0
            post_dispatches = 0

            @override
            def _read_post_body(self) -> dict[str, object] | None:
                type(self).body_reads += 1
                return super()._read_post_body()

            @override
            def do_POST(self) -> None:
                type(self).post_dispatches += 1
                super().do_POST()

        OrderingHandler.body_reads = 0
        OrderingHandler.post_dispatches = 0
        OrderingHandler._FUNC_POST_ROUTES = {
            "/api/__security_probe": service_route,
        }
        server = ThreadingHTTPServer(("127.0.0.1", 0), OrderingHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            url = (
                f"http://127.0.0.1:{server.server_port}"
                "/api/__security_probe"
            )
            request = Request(
                url,
                data=b'{"mutate":true}',
                headers={
                    "Content-Type": "application/json",
                    CHANNEL_HEADER: BROWSER_CHANNEL,
                    "Origin": "https://evil.example",
                },
                method="POST",
            )
            with self.assertRaises(HTTPError) as raised:
                urlopen(request, timeout=5)
            with raised.exception:
                self.assertEqual(raised.exception.code, 403)
                self.assertEqual(
                    json.loads(raised.exception.read()),
                    {"error": "Request rejected"},
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)

        self.assertEqual(OrderingHandler.body_reads, 0)
        self.assertEqual(OrderingHandler.post_dispatches, 0)
        self.assertEqual(route_calls, [])

    def test_youtube_resolver_rejects_bad_provenance_before_resolver_or_db(
        self,
    ) -> None:
        """The cache-writing resolver cannot cross the browser guard."""
        from web import server as srv

        class _ForbiddenDB:
            def __getattribute__(self, name: str) -> object:
                raise AssertionError(
                    f"rejected resolver request touched DB attribute {name}"
                )

        cases = (
            ("missing", {}),
            ("mismatched", {"Origin": "https://evil.example"}),
        )
        for label, provenance_headers in cases:
            headers = {
                CHANNEL_HEADER: BROWSER_CHANNEL,
                "Content-Type": "application/json",
                **provenance_headers,
            }
            with self.subTest(label=label), patch.object(
                srv, "db", _ForbiddenDB(),
            ), patch(
                "web.routes.youtube.resolve_youtube_album",
            ) as resolver:
                status, _body, _response_headers = self._raw_request(
                    "POST",
                    "/api/youtube-album",
                    headers=headers,
                    body=b'{"identifier":"release-id","refresh":false}',
                )
            self.assertEqual(status, 403)
            resolver.assert_not_called()

    def test_missing_and_unknown_channels_reject_before_get_dispatch(self) -> None:
        cases = (
            ("missing", {}),
            ("unknown", {CHANNEL_HEADER: "unknown"}),
        )
        for label, headers in cases:
            with self.subTest(label=label):
                status, _body, _response_headers = self._raw_request(
                    "GET", "/api/_index", headers=headers,
                )
                self.assertEqual(status, 403)

    def test_duplicate_security_headers_close_before_body_reparse(self) -> None:
        cases = (
            (
                "duplicate channel fields",
                (
                    f"{CHANNEL_HEADER}: browser\r\n"
                    f"{CHANNEL_HEADER}: cli\r\n"
                    f"Origin: {CANONICAL_ORIGIN}\r\n"
                ),
            ),
            (
                "serialized channel values",
                (
                    f"{CHANNEL_HEADER}: browser, cli\r\n"
                    f"Origin: {CANONICAL_ORIGIN}\r\n"
                ),
            ),
            (
                "duplicate Origin fields",
                (
                    f"{CHANNEL_HEADER}: browser\r\n"
                    f"Origin: {CANONICAL_ORIGIN}\r\n"
                    "Origin: https://evil.example\r\n"
                ),
            ),
            (
                "serialized Origin values",
                (
                    f"{CHANNEL_HEADER}: browser\r\n"
                    f"Origin: {CANONICAL_ORIGIN} https://evil.example\r\n"
                ),
            ),
        )
        for label, security_headers in cases:
            with self.subTest(label=label):
                request = (
                    "POST /api/nonexistent HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{self.port}\r\n"
                    f"{security_headers}"
                    "Content-Type: application/json\r\n"
                    "Content-Length: 2\r\n"
                    "Connection: keep-alive\r\n\r\n"
                    "{}"
                    "GET /healthz HTTP/1.1\r\n"
                    f"Host: 127.0.0.1:{self.port}\r\n\r\n"
                ).encode()
                with socket.create_connection(
                    ("127.0.0.1", self.port), timeout=5,
                ) as client:
                    client.sendall(request)
                    response = bytearray()
                    while chunk := client.recv(4096):
                        response.extend(chunk)
                response_bytes = bytes(response)
                self.assertTrue(response_bytes.startswith(b"HTTP/1.1 403"))
                self.assertEqual(response_bytes.count(b"HTTP/1.1 "), 1)

    def test_expected_origin_is_never_derived_from_request_host(self) -> None:
        status, _body, _headers = self._raw_request(
            "POST",
            "/api/nonexistent",
            headers={
                "Host": "evil.example",
                CHANNEL_HEADER: BROWSER_CHANNEL,
                "Origin": "https://evil.example",
                "Content-Type": "application/json",
            },
            body=b"{}",
        )
        self.assertEqual(status, 403)

    def test_exact_liveness_is_bare_and_dependency_free(self) -> None:
        from web import server as srv

        route_calls: list[object] = []

        def unexpected_route(_handler, _params) -> None:
            route_calls.append(object())

        original = srv.Handler._FUNC_GET_ROUTES.get("/healthz")
        original_db = srv.db
        original_beets = srv._beets
        srv.Handler._FUNC_GET_ROUTES["/healthz"] = unexpected_route
        srv.db = None
        srv._beets = None
        try:
            for method in ("GET", "HEAD"):
                with self.subTest(method=method):
                    status, body, headers = self._raw_request(
                        method, "/healthz",
                    )
                    self.assertEqual(status, 204)
                    self.assertEqual(body, b"")
                    self.assertIsNone(headers.get("Content-Length"))
                    self.assertIsNone(headers.get("Server"))
                    self.assertIsNone(headers.get("Date"))
        finally:
            if original is None:
                srv.Handler._FUNC_GET_ROUTES.pop("/healthz", None)
            else:
                srv.Handler._FUNC_GET_ROUTES["/healthz"] = original
            srv.db = original_db
            srv._beets = original_beets
        self.assertEqual(route_calls, [])

    def test_liveness_variants_are_not_anonymous(self) -> None:
        cases = (
            ("GET", "/healthz?probe=1"),
            ("GET", "/healthz?"),
            ("GET", "//healthz"),
            ("GET", "/healthz/"),
            ("GET", "/x/../healthz"),
            ("GET", "/health%7a"),
            ("POST", "/healthz"),
            ("OPTIONS", "/healthz"),
        )
        for method, target in cases:
            with self.subTest(method=method, target=target):
                if target.startswith("//"):
                    request = (
                        f"{method} {target} HTTP/1.1\r\n"
                        f"Host: 127.0.0.1:{self.port}\r\n"
                        "Connection: close\r\n\r\n"
                    ).encode()
                    with socket.create_connection(
                        ("127.0.0.1", self.port), timeout=5,
                    ) as client:
                        client.sendall(request)
                        response = bytearray()
                        while chunk := client.recv(4096):
                            response.extend(chunk)
                    status = int(bytes(response).split(b" ", 2)[1])
                else:
                    status, _body, _headers = self._raw_request(method, target)
                self.assertEqual(status, 403)

    def test_json_and_options_responses_publish_no_cors_contract(self) -> None:
        status, _body, headers = self._raw_request(
            "GET",
            "/api/_index",
            headers={CHANNEL_HEADER: BROWSER_CHANNEL},
        )
        self.assertEqual(status, 200)
        self.assertFalse(
            any(name.lower().startswith("access-control-") for name in headers)
        )

        status, _body, headers = self._raw_request(
            "OPTIONS",
            "/api/_index",
            headers={CHANNEL_HEADER: BROWSER_CHANNEL},
        )
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("Content-Length"), "0")
        self.assertFalse(
            any(name.lower().startswith("access-control-") for name in headers)
        )

    def test_route_inventory_keeps_registered_mutations_outside_safe_methods(
        self,
    ) -> None:
        from web.server import ALL_ROUTES

        registered_methods = {route.method for route in ALL_ROUTES}
        self.assertEqual(registered_methods - SAFE_METHODS, {"POST"})
        self.assertEqual(registered_methods & SAFE_METHODS, {"GET"})


if __name__ == "__main__":
    unittest.main()
