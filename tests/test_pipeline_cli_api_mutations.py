"""Pins for the CLI-over-HTTP mutation adapters (CD-QUAL-01)."""

from __future__ import annotations

import argparse
from email.message import Message
import io
import json
import threading
import unittest
import urllib.error
from contextlib import redirect_stdout, redirect_stderr
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

from scripts.pipeline_cli import api_mutations
from scripts.pipeline_cli.routes_meta import _build_parser
from tests.helpers import make_request_row
from tests.web._harness import _FakeDbWebServerCase


class _Response:
    def __init__(self, status: int, body: bytes = b'{"status":"ok"}') -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *_values: object) -> None:
        return None


class _RedirectingApiHandler(BaseHTTPRequestHandler):
    redirect_status = 301
    initial_methods: list[str] = []
    target_methods: list[str] = []

    def _json(self, status: int, payload: bytes, *, location: str | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
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
        self.assertEqual(parser.parse_args(["upgrade", "release"]).api_base,
                         "http://127.0.0.1:8085")

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
            ("pipeline-delete", {"api_base": "http://api", "request_id": 7,
             "confirm": "DELETE"}, "/api/pipeline/delete", {"id": 7}),
            ("set-quality", {"api_base": "http://api", "release_id": "r1",
             "status": "wanted", "min_bitrate": 320}, "/api/pipeline/set-quality",
             {"mb_release_id": "r1", "status": "wanted", "min_bitrate": 320}),
            ("upgrade", {"api_base": "http://api", "release_id": "r2"},
             "/api/pipeline/upgrade", {"mb_release_id": "r2"}),
            ("wrong-match-converge", {"api_base": "http://api", "request_id": 8,
             "threshold_milli": 150, "apply": True}, "/api/wrong-matches/converge",
             {"request_id": 8, "threshold_milli": 150}),
            ("resolve-rg", {"api_base": "http://api", "request_id": 9},
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

    def test_http_exit_classes_and_json_relay(self) -> None:
        for status, expected in [(200, 0), (201, 0), (404, 2), (400, 3),
                                 (422, 3), (409, 4), (500, 5)]:
            with self.subTest(status=status), patch(
                "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                return_value=_Response(status, b'{"error":"route"}'),
            ):
                code, output, _error = self._run(
                    "upgrade", api_base="http://api", release_id="r")
            self.assertEqual(code, expected)
            self.assertEqual(json.loads(output), {"error": "route"})

    def test_http_error_transport_and_malformed_responses_are_fail_closed(self) -> None:
        error = urllib.error.HTTPError("http://api", 404, "missing", Message(),
                                       io.BytesIO(b'{"error":"missing"}'))
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   side_effect=error):
            code, output, _stderr = self._run("upgrade", api_base="http://api", release_id="r")
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(output), {"error": "missing"})
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   return_value=_Response(200, b"not-json")):
            code, _output, stderr = self._run("upgrade", api_base="http://api", release_id="r")
        self.assertEqual(code, 5)
        self.assertEqual(json.loads(stderr)["error"], "api_protocol_error")
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                   side_effect=urllib.error.URLError("down")):
            code, _output, stderr = self._run("upgrade", api_base="http://api", release_id="r")
        self.assertEqual(code, 5)
        self.assertEqual(json.loads(stderr)["error"], "api_unavailable")

    def test_malformed_api_origins_are_structured_local_protocol_failures(self) -> None:
        for origin in ("http://[::1", "not an origin"):
            with self.subTest(origin=origin):
                code, output, stderr = self._run(
                    "upgrade", api_base=origin, release_id="r")
                self.assertEqual(code, 5)
                self.assertEqual(output, "")
                self.assertEqual(json.loads(stderr)["error"], "api_protocol_error")

    def test_confirmation_gates_make_no_http_call(self) -> None:
        with patch("scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open") as urlopen:
            code, _out, _err = self._run("pipeline-delete", api_base="http://api",
                                         request_id=1, confirm=None)
            self.assertEqual(code, 3)
            code, _out, _err = self._run("wrong-match-converge", api_base="http://api",
                                         request_id=1, threshold_milli=1, apply=False)
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
        for command in commands:
            with self.subTest(command=command), patch(
                "scripts.pipeline_cli.cli.PipelineDB", side_effect=AssertionError,
            ), patch(
                "web.api_bases.configure_api_bases_from_runtime_config",
                side_effect=AssertionError,
            ), patch(
                "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                return_value=_Response(200),
            ) as urlopen, patch("sys.argv", [
                "pipeline-cli", "--api-base", "http://api", *command,
            ]):
                with self.assertRaises(SystemExit) as exited:
                    from scripts.pipeline_cli.cli import main
                    main()
            self.assertEqual(exited.exception.code, 0)
            urlopen.assert_called_once()


class TestApiMutationRealRouteRoundTrips(_FakeDbWebServerCase):
    """Each adapter reaches the actual route dispatcher, not a route mock."""

    def _call(self, handler, **values: object) -> tuple[int, dict[str, object]]:
        stdout = io.StringIO()
        with redirect_stdout(stdout):
            code = handler(None, argparse.Namespace(api_base=self.base, **values))
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


class TestApiMutationRedirectSafety(unittest.TestCase):
    """A redirect must remain the route's response, never a second request."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.server = HTTPServer(("127.0.0.1", 0), _RedirectingApiHandler)
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def test_redirects_are_not_followed_or_replayed(self) -> None:
        for status in (301, 302, 303, 307, 308):
            with self.subTest(status=status):
                _RedirectingApiHandler.redirect_status = status
                _RedirectingApiHandler.initial_methods.clear()
                _RedirectingApiHandler.target_methods.clear()
                stdout = io.StringIO()
                stderr = io.StringIO()
                with redirect_stdout(stdout), redirect_stderr(stderr):
                    code = api_mutations._relay(self.base, api_mutations._ApiMutation(
                        path="/initial", body={"request_id": 1}))
                self.assertEqual(code, 5)
                self.assertEqual(json.loads(stdout.getvalue()), {"error": "redirect"})
                self.assertEqual(stderr.getvalue(), "")
                self.assertEqual(_RedirectingApiHandler.initial_methods, ["POST"])
                self.assertEqual(_RedirectingApiHandler.target_methods, [])
