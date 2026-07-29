"""Pins for the CLI-over-HTTP mutation adapters (CD-QUAL-01)."""

from __future__ import annotations

import argparse
import io
import json
import os
import socket
import tempfile
import threading
import unittest
import urllib.error
from contextlib import redirect_stderr, redirect_stdout
from email.message import Message
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import ClassVar, Self
from unittest.mock import patch

from scripts.pipeline_cli import api_mutations
from scripts.pipeline_cli.routes_meta import _build_parser
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase


class _Response:
    def __init__(self, status: int, body: bytes = b'{"status":"ok"}') -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_values: object) -> None:
        return None


class _RedirectingApiHandler(BaseHTTPRequestHandler):
    redirect_status = 301
    initial_methods: ClassVar[list[str]] = []
    target_methods: ClassVar[list[str]] = []

    def _json(self, status: int, payload: bytes, *, location: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        if location is not None:
            self.send_header("Location", location)
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        if self.path == "/initial":
            self.initial_methods.append("POST")
            self._json(self.redirect_status, b'{"error":"redirect"}',
                       location="/target")
            return
        self.target_methods.append("POST")
        self._json(200, b'{"status":"redirected"}')

    def do_GET(self) -> None:
        self.target_methods.append("GET")
        self._json(200, b'{"status":"redirected"}')

    def log_message(self, format: str, *_args: object) -> None:
        return None


class TestApiMutationCli(unittest.TestCase):
    def test_standalone_api_origin_matches_module_default(self) -> None:
        parser, _search_plan, _triage = _build_parser()
        self.assertEqual(api_mutations.DEFAULT_API_BASE, "http://127.0.0.1:8085")
        self.assertEqual(
            parser.parse_args(["upgrade", "release"]).api_endpoint,
            api_mutations.TcpApiEndpoint("http://127.0.0.1:8085"),
        )

    def test_installed_socket_parser_has_no_api_base_override(self) -> None:
        parser, _search_plan, _triage = _build_parser(
            api_socket="/run/cratedigger/web.sock",
        )
        args = parser.parse_args(["upgrade", "release"])
        self.assertEqual(
            args.api_endpoint,
            api_mutations.UnixApiEndpoint("/run/cratedigger/web.sock"),
        )
        with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            parser.parse_args([
                "upgrade",
                "release",
                "--api-base",
                "http://attacker.invalid",
            ])

    def test_youtube_album_parser_preserves_refresh_and_json_flags(self) -> None:
        parser, _search_plan, _triage = _build_parser()
        defaults = parser.parse_args(["youtube-album", "release-3"])
        self.assertFalse(defaults.refresh)
        self.assertFalse(defaults.json)
        args = parser.parse_args([
            "youtube-album",
            "release-3",
            "--refresh",
            "--json",
        ])
        self.assertTrue(args.refresh)
        self.assertTrue(args.json)

    def _run(self, command: str, **values: object) -> tuple[int, str, str]:
        handlers = {
            "pipeline-delete": api_mutations.cmd_pipeline_delete,
            "set-quality": api_mutations.cmd_set_quality,
            "upgrade": api_mutations.cmd_upgrade,
            "wrong-match-converge": api_mutations.cmd_wrong_match_converge,
            "resolve-rg": api_mutations.cmd_resolve_rg,
        }
        stdout = io.StringIO()
        stderr = io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            result = handlers[command](None, argparse.Namespace(**values))
        return result, stdout.getvalue(), stderr.getvalue()

    def test_each_command_preserves_canonical_method_path_and_body(self) -> None:
        cases = [
            ("pipeline-delete", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "request_id": 7,
             "confirm": "DELETE"}, "/api/pipeline/delete", {"id": 7}),
            ("set-quality", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "release_id": "r1",
             "status": "wanted", "min_bitrate": 320}, "/api/pipeline/set-quality",
             {"mb_release_id": "r1", "status": "wanted", "min_bitrate": 320}),
            ("upgrade", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "release_id": "r2"},
             "/api/pipeline/upgrade", {"mb_release_id": "r2"}),
            ("wrong-match-converge", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "request_id": 8,
             "threshold_milli": 150, "apply": True}, "/api/wrong-matches/converge",
             {"request_id": 8, "threshold_milli": 150}),
            ("resolve-rg", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "request_id": 9},
             "/api/pipeline/9/resolve-rg", {}),
        ]
        for command, values, path, body in cases:
            with self.subTest(command=command), patch(
                "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                return_value=_Response(200),
            ) as urlopen:
                code, output, error = self._run(command, **values)
            self.assertEqual(code, 0)
            self.assertEqual(json.loads(output), {"status": "ok"})
            self.assertEqual(error, "")
            request = urlopen.call_args.args[0]
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.full_url, f"http://api{path}")
            self.assertEqual(json.loads(request.data or b""), body)
            self.assertEqual(
                request.get_header("X-cratedigger-request-channel"),
                "cli",
            )

    def test_http_exit_classes_and_json_relay(self) -> None:
        for status, expected in [(200, 0), (201, 0), (404, 2), (400, 3),
                                 (422, 3), (409, 4), (500, 5)]:
            with self.subTest(status=status), patch(
                "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                return_value=_Response(status, b'{"error":"route"}'),
            ):
                code, output, _error = self._run(
                    "upgrade",
                    api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                    release_id="r",
                )
            self.assertEqual(code, expected)
            self.assertEqual(json.loads(output), {"error": "route"})

    def test_http_error_transport_and_malformed_responses_are_fail_closed(self) -> None:
        error = urllib.error.HTTPError("http://api", 404, "missing", Message(),
                                       io.BytesIO(b'{"error":"missing"}'))
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   side_effect=error):
            code, output, _stderr = self._run(
                "upgrade",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                release_id="r",
            )
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output), {"error": "missing"})
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   return_value=_Response(200, b"not-json")):
            code, _output, stderr = self._run(
                "upgrade",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                release_id="r",
            )
        self.assertEqual(code, 5)
        self.assertEqual(json.loads(stderr)["error"], "api_protocol_error")
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   side_effect=urllib.error.URLError("down")):
            code, _output, stderr = self._run(
                "upgrade",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                release_id="r",
            )
        self.assertEqual(code, 5)
        self.assertEqual(json.loads(stderr)["error"], "api_unavailable")

    def test_malformed_api_origins_are_structured_local_protocol_failures(self) -> None:
        for origin in ("http://[::1", "not an origin"):
            with self.subTest(origin=origin):
                code, output, stderr = self._run(
                    "upgrade",
                    api_endpoint=api_mutations.TcpApiEndpoint(origin),
                    release_id="r",
                )
                self.assertEqual(code, 5)
                self.assertEqual(output, "")
                self.assertEqual(json.loads(stderr)["error"], "api_protocol_error")

    def test_confirmation_gates_make_no_http_call(self) -> None:
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open") as urlopen:
            code, _out, _err = self._run(
                "pipeline-delete",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                request_id=1,
                confirm=None,
            )
            self.assertEqual(code, 3)
            code, _out, _err = self._run(
                "wrong-match-converge",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                request_id=1,
                threshold_milli=1,
                apply=False,
            )
        self.assertEqual(code, 3)
        urlopen.assert_not_called()

    def test_all_api_commands_dispatch_before_db_and_mirror_setup(self) -> None:
        commands = [
            ["pipeline-delete", "7", "--confirm", "DELETE"],
            ["set-quality", "release-1", "--status", "wanted"],
            ["upgrade", "release-2"],
            ["wrong-match-converge", "8", "150", "--apply"],
            ["resolve-rg", "9"],
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.ini")
            # Missing config deliberately soft-fails. This existing but invalid
            # config, plus the invalid DSN below, makes either forbidden setup
            # path fail loudly if an API command stops short-circuiting.
            with open(config_path, "w", encoding="utf-8") as config_file:
                config_file.write(
                    "[Search Settings]\n"
                    "number_of_albums_to_grab = 1\n",
                )
            for command in commands:
                with self.subTest(command=command), patch.dict(
                    os.environ, {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
                ), patch(
                    "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                    return_value=_Response(200),
                ) as urlopen, patch("sys.argv", [
                    "pipeline-cli", "--dsn", "not-a-postgresql-dsn",
                    "--api-base", "http://api", *command,
                ]), self.assertRaises(SystemExit) as exited:
                    from scripts.pipeline_cli.cli import main
                    main()
                self.assertEqual(exited.exception.code, 0)
                urlopen.assert_called_once()

    def test_youtube_album_stays_headless_when_web_socket_is_missing(self) -> None:
        """The resolver is a direct service adapter, not a sixth API command."""
        from lib.youtube_album_service import YoutubeAlbumResolverResult
        from scripts.pipeline_cli.cli import main

        class _Session:
            def close(self) -> None:
                pass

        result = YoutubeAlbumResolverResult(
            outcome="ok",
            release_group_identifier="group-3",
            source="mb",
            from_cache=False,
            youtube_releases=[],
            duration_ms=12,
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.ini")
            with open(config_path, "w", encoding="utf-8") as config_file:
                config_file.write(
                    "[MusicBrainz]\n"
                    "api_base = http://mb.test\n"
                    "[Discogs]\n"
                    "api_base = http://discogs.test\n",
                )
            with patch.dict(
                os.environ,
                {"CRATEDIGGER_RUNTIME_CONFIG": config_path},
            ), patch(
                "sys.argv",
                ["pipeline-cli", "youtube-album", "release-3", "--json"],
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB",
                return_value=FakePipelineDB(),
            ), patch(
                "scripts.pipeline_cli.youtube._build_youtube_client",
                return_value=(object(), _Session()),
            ), patch(
                "scripts.pipeline_cli.youtube._RedisYoutubeCache",
                return_value=object(),
            ), patch(
                "scripts.pipeline_cli.youtube.resolve_youtube_album",
                return_value=result,
            ) as resolve, patch(
                "scripts.pipeline_cli.api_mutations.urllib.request."
                "OpenerDirector.open",
            ) as api_open, redirect_stdout(io.StringIO()) as stdout, \
                    self.assertRaises(SystemExit) as exited:
                main(api_socket=os.path.join(temp_dir, "missing.sock"))

        self.assertEqual(exited.exception.code, 0)
        self.assertEqual(json.loads(stdout.getvalue())["outcome"], "ok")
        resolve.assert_called_once()
        api_open.assert_not_called()

    def test_unix_socket_failures_are_structured_and_never_fall_back_to_tcp(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
        ) as tcp_open:
            missing = os.path.join(temp_dir, "missing.sock")
            code, output, stderr = self._run(
                "upgrade",
                api_endpoint=api_mutations.UnixApiEndpoint(missing),
                release_id="r",
            )
        self.assertEqual(code, 5)
        self.assertEqual(output, "")
        self.assertEqual(json.loads(stderr)["error"], "api_unavailable")
        tcp_open.assert_not_called()

    def test_stale_unix_socket_node_is_a_structured_connection_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "stale.sock")
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(path)
            listener.close()
            code, output, stderr = self._run(
                "upgrade",
                api_endpoint=api_mutations.UnixApiEndpoint(path),
                release_id="r",
            )
        self.assertEqual(code, 5)
        self.assertEqual(output, "")
        self.assertEqual(json.loads(stderr)["error"], "api_unavailable")

    def test_unix_permission_denial_is_a_structured_local_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "api.sock")
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            listener.bind(path)
            listener.listen()
            os.chmod(temp_dir, 0)
            try:
                code, output, stderr = self._run(
                    "upgrade",
                    api_endpoint=api_mutations.UnixApiEndpoint(path),
                    release_id="r",
                )
            finally:
                os.chmod(temp_dir, 0o700)
                listener.close()
        self.assertEqual(code, 5)
        self.assertEqual(output, "")
        self.assertEqual(json.loads(stderr)["error"], "api_unavailable")

    def test_unix_timeout_and_malformed_http_fail_closed(self) -> None:
        def exercise_raw_server(
            response: bytes | None,
            *,
            timeout_seconds: float,
        ) -> tuple[int, str]:
            with tempfile.TemporaryDirectory() as temp_dir:
                path = os.path.join(temp_dir, "raw.sock")
                listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                listener.bind(path)
                listener.listen()

                def serve() -> None:
                    connection, _address = listener.accept()
                    try:
                        # Drain the complete request before closing the socket.
                        # A single recv() can stop between headers and body;
                        # closing with unread request bytes then races into an
                        # ECONNRESET, masking a deliberately malformed HTTP
                        # status line as a transport failure under load.
                        with connection.makefile("rb") as request_stream:
                            content_length = 0
                            while True:
                                line = request_stream.readline()
                                if line == b"\r\n":
                                    break
                                if line.lower().startswith(b"content-length:"):
                                    content_length = int(
                                        line.split(b":", 1)[1].strip(),
                                    )
                            request_stream.read(content_length)
                        if response is None:
                            threading.Event().wait(timeout_seconds * 4)
                        else:
                            connection.sendall(response)
                    finally:
                        connection.close()
                        listener.close()

                thread = threading.Thread(target=serve)
                thread.start()
                stderr = io.StringIO()
                with redirect_stderr(stderr):
                    code = api_mutations._relay(
                        api_mutations.UnixApiEndpoint(path),
                        api_mutations._ApiMutation(
                            path="/api/pipeline/upgrade",
                            body={"mb_release_id": "r"},
                        ),
                        timeout_seconds=timeout_seconds,
                    )
                thread.join(timeout=5)
                self.assertFalse(thread.is_alive())
                return code, stderr.getvalue()

        timeout_code, timeout_stderr = exercise_raw_server(
            None,
            timeout_seconds=0.05,
        )
        self.assertEqual(timeout_code, 5)
        self.assertEqual(
            json.loads(timeout_stderr)["error"],
            "api_unavailable",
        )

        disconnect_code, disconnect_stderr = exercise_raw_server(
            b"",
            timeout_seconds=1.0,
        )
        self.assertEqual(disconnect_code, 5)
        self.assertEqual(
            json.loads(disconnect_stderr)["error"],
            "api_unavailable",
        )

        malformed_code, malformed_stderr = exercise_raw_server(
            b"not-http\r\n\r\n",
            timeout_seconds=1.0,
        )
        self.assertEqual(malformed_code, 5)
        self.assertEqual(
            json.loads(malformed_stderr)["error"],
            "api_protocol_error",
        )

    def test_installed_unix_failure_short_circuits_before_runtime_and_db(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "sys.argv",
            ["pipeline-cli", "--dsn", "not-a-postgresql-dsn",
             "upgrade", "release-2"],
        ), redirect_stderr(io.StringIO()), self.assertRaises(
            SystemExit,
        ) as exited:
            from scripts.pipeline_cli.cli import main

            main(api_socket=os.path.join(temp_dir, "missing.sock"))
        self.assertEqual(exited.exception.code, 5)


class TestApiMutationRealRouteRoundTrips(_FakeDbWebServerCase):
    """Each adapter reaches the actual route dispatcher, not a route mock."""

    def _call(self, handler, **values: object) -> tuple[int, dict[str, object]]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = handler(
                None,
                argparse.Namespace(
                    api_endpoint=api_mutations.TcpApiEndpoint(self.base),
                    **values,
                ),
            )
        return code, json.loads(stdout.getvalue())

    def _seed(self, request_id: int, release_id: str) -> None:
        self.db.seed_request(make_request_row(id=request_id, mb_release_id=release_id))

    def test_all_five_adapters_round_trip_through_web_harness(self) -> None:
        self._seed(101, "a0000000-0000-0000-0000-000000000001")
        code, body = self._call(api_mutations.cmd_pipeline_delete, request_id=101,
                                confirm="DELETE")
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(102, "a0000000-0000-0000-0000-000000000002")
        code, body = self._call(api_mutations.cmd_set_quality,
                                release_id="a0000000-0000-0000-0000-000000000002",
                                status="wanted", min_bitrate=320)
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(103, "a0000000-0000-0000-0000-000000000003")
        code, body = self._call(api_mutations.cmd_upgrade,
                                release_id="a0000000-0000-0000-0000-000000000003")
        self.assertEqual((code, body["status"]), (0, "upgrade_queued"))

        self._seed(104, "a0000000-0000-0000-0000-000000000004")
        code, body = self._call(api_mutations.cmd_wrong_match_converge,
                                request_id=104, threshold_milli=150, apply=True)
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(105, "a0000000-0000-0000-0000-000000000005")
        self.db.update_request_fields(105, mb_release_group_id="rg-already-set")
        code, body = self._call(api_mutations.cmd_resolve_rg, request_id=105)
        self.assertEqual((code, body["status"]), (0, "resolved"))


class TestApiMutationUnixRealRouteRoundTrips(unittest.TestCase):
    """All five adapters use real HTTP and route dispatch over AF_UNIX."""

    def setUp(self) -> None:
        from web import server as srv

        self._srv = srv
        self._saved_db = srv.db
        self._saved_dsn = srv._db_dsn
        self._saved_origin = srv.canonical_origin
        self.db = FakePipelineDB()
        srv.db = self.db
        srv._db_dsn = None
        srv.canonical_origin = "https://music.ablz.au"

        class RecordingHandler(srv.Handler):
            observations: ClassVar[list[tuple[str, str, str | None, str | None]]] = []

            def parse_request(self) -> bool:
                accepted = super().parse_request()
                if accepted:
                    self.observations.append((
                        self.command,
                        self.path,
                        self.headers.get("Host"),
                        self.headers.get("X-Cratedigger-Request-Channel"),
                    ))
                return accepted

        self.handler = RecordingHandler
        self._temp_dir = tempfile.TemporaryDirectory()
        self.socket_path = os.path.join(self._temp_dir.name, "api.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(self.socket_path)
        listener.listen()
        self.server = srv.ThreadingUnixHTTPServer(listener, RecordingHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        self._srv.db = self._saved_db
        self._srv._db_dsn = self._saved_dsn
        self._srv.canonical_origin = self._saved_origin
        self._temp_dir.cleanup()

    def _call(self, handler, **values: object) -> tuple[int, dict[str, object]]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = handler(
                None,
                argparse.Namespace(
                    api_endpoint=api_mutations.UnixApiEndpoint(
                        self.socket_path,
                    ),
                    **values,
                ),
            )
        return code, json.loads(stdout.getvalue())

    def _seed(self, request_id: int, release_id: str) -> None:
        self.db.seed_request(
            make_request_row(id=request_id, mb_release_id=release_id),
        )

    def test_all_five_adapters_preserve_routes_bodies_and_cli_authority(
        self,
    ) -> None:
        self._seed(201, "b0000000-0000-0000-0000-000000000001")
        code, body = self._call(
            api_mutations.cmd_pipeline_delete,
            request_id=201,
            confirm="DELETE",
        )
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(202, "b0000000-0000-0000-0000-000000000002")
        code, body = self._call(
            api_mutations.cmd_set_quality,
            release_id="b0000000-0000-0000-0000-000000000002",
            status="wanted",
            min_bitrate=320,
        )
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(203, "b0000000-0000-0000-0000-000000000003")
        code, body = self._call(
            api_mutations.cmd_upgrade,
            release_id="b0000000-0000-0000-0000-000000000003",
        )
        self.assertEqual((code, body["status"]), (0, "upgrade_queued"))

        self._seed(204, "b0000000-0000-0000-0000-000000000004")
        code, body = self._call(
            api_mutations.cmd_wrong_match_converge,
            request_id=204,
            threshold_milli=150,
            apply=True,
        )
        self.assertEqual((code, body["status"]), (0, "ok"))

        self._seed(205, "b0000000-0000-0000-0000-000000000005")
        self.db.update_request_fields(
            205,
            mb_release_group_id="rg-already-set",
        )
        code, body = self._call(
            api_mutations.cmd_resolve_rg,
            request_id=205,
        )
        self.assertEqual((code, body["status"]), (0, "resolved"))

        code, body = self._call(
            api_mutations.cmd_pipeline_delete,
            request_id=999,
            confirm="DELETE",
        )
        self.assertEqual((code, body), (2, {"error": "Not found"}))

        self.assertEqual(
            [(method, path) for method, path, _host, _channel
             in self.handler.observations],
            [
                ("POST", "/api/pipeline/delete"),
                ("POST", "/api/pipeline/set-quality"),
                ("POST", "/api/pipeline/upgrade"),
                ("POST", "/api/wrong-matches/converge"),
                ("POST", "/api/pipeline/205/resolve-rg"),
                ("POST", "/api/pipeline/delete"),
            ],
        )
        self.assertEqual(
            {host for _method, _path, host, _channel
             in self.handler.observations},
            {"cratedigger.internal"},
        )
        self.assertEqual(
            {channel for _method, _path, _host, channel
             in self.handler.observations},
            {"cli"},
        )


class TestApiMutationRedirectSafety(unittest.TestCase):
    """A redirect must remain the route's response, never a second request."""

    @classmethod
    def setUpClass(cls) -> None:
        from web.server import ThreadingUnixHTTPServer

        cls.server = HTTPServer(("127.0.0.1", 0), _RedirectingApiHandler)
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.start()

        cls.unix_temp_dir = tempfile.TemporaryDirectory()
        cls.unix_path = os.path.join(
            cls.unix_temp_dir.name,
            "redirect.sock",
        )
        unix_listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        unix_listener.bind(cls.unix_path)
        unix_listener.listen()
        cls.unix_server = ThreadingUnixHTTPServer(
            unix_listener,
            _RedirectingApiHandler,
        )
        cls.unix_thread = threading.Thread(
            target=cls.unix_server.serve_forever,
        )
        cls.unix_thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

        cls.unix_server.shutdown()
        cls.unix_server.server_close()
        cls.unix_thread.join()
        cls.unix_temp_dir.cleanup()

    def test_redirects_are_not_followed_or_replayed(self) -> None:
        self._assert_redirects_are_not_followed_or_replayed(
            api_mutations.TcpApiEndpoint(self.base),
        )

    def test_unix_redirects_are_not_followed_or_replayed(self) -> None:
        self._assert_redirects_are_not_followed_or_replayed(
            api_mutations.UnixApiEndpoint(self.unix_path),
        )

    def _assert_redirects_are_not_followed_or_replayed(
        self,
        endpoint: api_mutations.ApiEndpoint,
    ) -> None:
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                _RedirectingApiHandler.redirect_status = status
                _RedirectingApiHandler.initial_methods.clear()
                _RedirectingApiHandler.target_methods.clear()
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = api_mutations._relay(
                        endpoint,
                        api_mutations._ApiMutation(
                            path="/initial",
                            body={"request_id": 1},
                        ),
                    )
                self.assertEqual(code, 5)
                self.assertEqual(json.loads(stdout.getvalue()), {"error": "redirect"})
                self.assertEqual(stderr.getvalue(), "")
                self.assertEqual(_RedirectingApiHandler.initial_methods, ["POST"])
                self.assertEqual(_RedirectingApiHandler.target_methods, [])
