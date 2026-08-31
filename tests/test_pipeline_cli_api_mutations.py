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

from scripts import pipeline_cli
from scripts.pipeline_cli import api_mutations
from scripts.pipeline_cli.routes_meta import _build_parser
from tests.dispatch_helpers import handoff_automation_owner
from tests.fakes import FakePipelineDB, FakeYTMusic
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


class _JsonEchoHandler(BaseHTTPRequestHandler):
    """Answers any method with a JSON object; used for deadline pins."""

    def _respond(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        if length:
            self.rfile.read(length)
        body = b'{"state": "completed", "summary": {}, "error": null}'
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_POST = _respond
    do_GET = _respond

    def log_message(self, format: str, *_args: object) -> None:
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
        content_length = int(self.headers.get("Content-Length", "0"))
        if content_length:
            self.rfile.read(content_length)
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
            "merge-rekey": api_mutations.cmd_merge_rekey,
            "sync-file-tags": api_mutations.cmd_sync_file_tags,
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
            ("merge-rekey", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "request_id": 316},
             "/api/pipeline/316/merge-rekey", {}),
            ("sync-file-tags", {"api_endpoint": api_mutations.TcpApiEndpoint(
                "http://api"), "album_id": 16948,
             "expected_mb_albumid": "26693e58-02c0-4bb1-b66f-f0f44f8a234d"},
             "/api/audit/retag-divergence/album/16948/sync-tags",
             {"expected_mb_albumid": "26693e58-02c0-4bb1-b66f-f0f44f8a234d"}),
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

    def test_pipeline_delete_relays_exact_processing_owner_at_exit_4(self) -> None:
        conflict = {
            "error": "transition_conflict",
            "reason": "processing_locked",
            "request_id": 42,
            "expected_status": "processing",
            "actual_status": "processing",
            "target_status": "deleted",
            "processing_owner": {
                "job_id": 81,
                "status": "running",
                "preview_status": "evidence_ready",
            },
        }
        with patch(
            "scripts.pipeline_cli.api_mutations.urllib.request."
            "OpenerDirector.open",
            return_value=_Response(409, json.dumps(conflict).encode()),
        ):
            code, output, error = self._run(
                "pipeline-delete",
                api_endpoint=api_mutations.TcpApiEndpoint("http://api"),
                request_id=42,
                confirm="DELETE",
            )

        self.assertEqual(code, 4)
        self.assertEqual(json.loads(output), conflict)
        self.assertEqual(error, "")

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
            ["merge-rekey", "316"],
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
        from scripts.pipeline_cli.cli import main

        release_group_id = "44438bf9-26d9-4460-9b4f-1a1b015e37a1"

        class _Session:
            def __init__(self) -> None:
                self.close_calls = 0

            def close(self) -> None:
                self.close_calls += 1

        class _NoopRedisCache:
            def get(self, _key: str) -> None:
                return None

            def set(
                self,
                _key: str,
                _value: bytes,
                _ttl_seconds: int,
            ) -> None:
                return None

        pdb = FakePipelineDB()
        pdb.seed_youtube_album_mapping(
            release_group_id,
            "mb",
            [
                {
                    "yt_browse_id": "MPREb-headless-cache",
                    "yt_audio_playlist_id": "OLAK5uy-headless-cache",
                    "yt_url": (
                        "https://music.youtube.com/playlist"
                        "?list=OLAK5uy-headless-cache"
                    ),
                    "yt_year": 1996,
                    "yt_track_count": 1,
                    "album_title": "Headless Album",
                    "album_artist": "Headless Artist",
                    "yt_tracks": [
                        {
                            "title": "Cached Track",
                            "artists": [{"name": "Headless Artist"}],
                            "length_seconds": 180.0,
                            "track_number": 1,
                            "disc_number": 1,
                            "video_id": "cached-video",
                        },
                    ],
                    "distances": [
                        {
                            "mbid": release_group_id,
                            "outcome": "ok",
                            "distance": 0.05,
                            "components": {"tracks": 0.05},
                            "matched_tracks": 1,
                            "total_local_tracks": 1,
                            "total_mb_tracks": 1,
                            "extra_local_tracks": 0,
                            "extra_mb_tracks": 0,
                        },
                    ],
                },
            ],
        )
        yt = FakeYTMusic()
        session = _Session()
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
                [
                    "pipeline-cli",
                    "youtube-album",
                    release_group_id,
                    "--json",
                ],
            ), patch(
                "scripts.pipeline_cli.cli.PipelineDB",
                return_value=pdb,
            ), patch(
                "scripts.pipeline_cli.youtube._build_youtube_client",
                return_value=(yt, session),
            ), patch(
                "scripts.pipeline_cli.youtube._RedisYoutubeCache",
                return_value=_NoopRedisCache(),
            ), patch(
                "web.mb.get_release",
                side_effect=AssertionError(
                    "cached dispatch must not fetch an MB release",
                ),
            ) as mb_release, patch(
                "web.mb.get_release_group_releases",
                side_effect=AssertionError(
                    "cached dispatch must not fetch an MB release group",
                ),
            ) as mb_group, patch(
                "web.discogs.get_release",
                side_effect=AssertionError(
                    "cached dispatch must not fetch a Discogs release",
                ),
            ) as discogs_release, patch(
                "web.discogs.get_master_releases",
                side_effect=AssertionError(
                    "cached dispatch must not fetch a Discogs master",
                ),
            ) as discogs_master, patch(
                "scripts.pipeline_cli.api_mutations.urllib.request."
                "OpenerDirector.open",
            ) as api_open, redirect_stdout(io.StringIO()) as stdout, \
                    self.assertRaises(SystemExit) as exited:
                main(api_socket=os.path.join(temp_dir, "missing.sock"))

        self.assertEqual(exited.exception.code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            {
                "outcome": payload["outcome"],
                "from_cache": payload["from_cache"],
                "source": payload["source"],
                "release_group_identifier": (
                    payload["release_group_identifier"]
                ),
                "browse_ids": [
                    row["yt_browse_id"]
                    for row in payload["youtube_releases"]
                ],
            },
            {
                "outcome": "ok",
                "from_cache": True,
                "source": "mb",
                "release_group_identifier": release_group_id,
                "browse_ids": ["MPREb-headless-cache"],
            },
        )
        self.assertEqual(yt.search_calls, [])
        self.assertEqual(yt.get_album_calls, [])
        self.assertEqual(session.close_calls, 1)
        mb_release.assert_not_called()
        mb_group.assert_not_called()
        discogs_release.assert_not_called()
        discogs_master.assert_not_called()
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

    def test_merge_rekey_reaches_the_real_route(self) -> None:
        """A sixth adapter (#1089), tested standalone: its happy path needs
        Beets + mirror fixtures the other five don't, so it gets its own
        real-route round trip rather than folding into the five above."""
        from lib.mb_canonical import (
            configure_canonical_base,
            configured_canonical_base,
        )
        from lib.quality import AlbumQualityEvidenceFile
        from tests.evidence_helpers import make_album_quality_evidence
        from tests.fakes import FakeBeetsDB

        merged = "6b209cc5-62b0-4ef7-9336-c2dbd876301a"
        survivor = "9b59f78b-3ca6-41e1-8025-6ed4bcfad4e4"
        self.db.seed_request(make_request_row(
            id=316, mb_release_id=merged, status="imported",
            artist_name="Rebecca Black", album_title="Sing It",
        ))
        import web.server as srv

        with tempfile.TemporaryDirectory() as tmp_dir:
            # #1089 MAJOR-C (review round 3): the evidence-lineage witness
            # is now mandatory, so the happy path needs real, MATCHING
            # bytes at the survivor — not merely a Beets album-id seed.
            real_path = os.path.join(tmp_dir, "01 Track.mp3")
            with open(real_path, "wb") as handle:
                handle.write(b"\x00" * 4096)
            evidence = make_album_quality_evidence(
                mb_release_id=merged,
                source_path=tmp_dir,
                files=[AlbumQualityEvidenceFile(
                    relative_path="01 Track.mp3", size_bytes=4096,
                    mtime_ns=1_700_000_000_000_000_000,
                    extension="mp3", container="mp3", codec="mp3",
                )],
            )
            self.db.upsert_album_quality_evidence(evidence)
            stored = self.db.find_album_quality_evidence(
                mb_release_id=merged,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert stored is not None and stored.id is not None
            self.assertTrue(
                self.db.set_request_current_evidence(316, stored.id),
            )

            beets = FakeBeetsDB()
            beets.set_album_ids_for_release(survivor, [19345])
            beets.set_item_paths(survivor, [(19345, real_path)])

            previous_base = configured_canonical_base()
            self.addCleanup(configure_canonical_base, previous_base)
            configure_canonical_base("http://fake-mirror/ws/2")
            with (
                patch.object(srv, "_beets_db", return_value=beets),
                # The TRUE external edge (#1089 NOTE-2, review round 2). The
                # service's default seam is the TAGGED resolver
                # (production_tagged_canonical_release_fn), which calls
                # canonical_release_status — NOT the collapsed
                # canonical_release_id the import seam uses (#1089 BLOCKING-1)
                # — but canonical_release_status is ~50 lines of real decision
                # logic, not a thin forwarder, so it is not allowlisted; this
                # patches the raw fetch one hop below it instead, exactly
                # mirroring the real ``{"payload": ..., "redirected": ...}``
                # envelope ``_fetch_json`` produces.
                patch(
                    "lib.mb_canonical._fetch_json",
                    return_value={
                        "payload": {"id": survivor}, "redirected": True,
                    },
                ),
            ):
                code, body = self._call(
                    api_mutations.cmd_merge_rekey, request_id=316,
                )

        self.assertEqual(code, 0)
        self.assertEqual(body["outcome"], "rekeyed")
        self.assertEqual(body["new_release_id"], survivor)
        self.assertEqual(self.db.request(316)["mb_release_id"], survivor)

    def test_sync_file_tags_reaches_the_real_route(self) -> None:
        """The seventh adapter (#1260): a 409 identity_mismatch round trip
        — the compare-and-set refusal — which needs only a Beets identity
        seed and never launches the write subprocess."""
        import web.server as srv
        from lib.beets_db import BeetsAlbumIdentityRow
        from tests.fakes import FakeBeetsDB

        beets = FakeBeetsDB()
        beets.set_album_mb_identities([
            BeetsAlbumIdentityRow(
                album_id=42,
                mb_albumid="26693e58-02c0-4bb1-b66f-f0f44f8a234d",
                item_paths=(),
            ),
        ])
        with patch.object(srv, "_beets_db", return_value=beets):
            code, body = self._call(
                api_mutations.cmd_sync_file_tags,
                album_id=42,
                expected_mb_albumid="fdc54a6a-27c7-4936-87d7-7ab146812d4e",
            )

        self.assertEqual(code, 4)
        self.assertEqual(body["outcome"], "identity_mismatch")
        self.assertEqual(
            body["db_mb_albumid"], "26693e58-02c0-4bb1-b66f-f0f44f8a234d",
        )

    def test_library_census_refresh_writes_trigger_and_maps_exit_codes(
        self,
    ) -> None:
        """The census force button's relay: 200→0 with the trigger file
        really written; unconfigured snapshot path relays 503→5."""
        import tempfile

        import web.server as srv
        from lib.library_completeness_snapshot import (
            library_completeness_snapshot_path,
            library_completeness_trigger_path,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            snapshot_path = library_completeness_snapshot_path(tmpdir)
            previous = srv.library_completeness_snapshot_path
            srv.library_completeness_snapshot_path = snapshot_path
            try:
                code, body = self._call(api_mutations.cmd_library_census_refresh)
                trigger_exists = os.path.exists(
                    library_completeness_trigger_path(tmpdir),
                )
            finally:
                srv.library_completeness_snapshot_path = previous
        self.assertEqual(code, 0)
        self.assertEqual(body["outcome"], "requested")
        self.assertTrue(trigger_exists)

        previous = srv.library_completeness_snapshot_path
        srv.library_completeness_snapshot_path = None
        try:
            code, body = self._call(api_mutations.cmd_library_census_refresh)
        finally:
            srv.library_completeness_snapshot_path = previous
        self.assertEqual(code, 5)
        self.assertEqual(body["outcome"], "unconfigured")

    def test_pipeline_delete_round_trip_preserves_processing_owner(self) -> None:
        self._seed(106, "a0000000-0000-0000-0000-000000000006")
        owner = handoff_automation_owner(self.db, 106)

        code, body = self._call(
            api_mutations.cmd_pipeline_delete,
            request_id=106,
            confirm="DELETE",
        )

        self.assertEqual(code, 4)
        self.assertEqual(body["reason"], "processing_locked")
        self.assertEqual(body["request_id"], 106)
        self.assertEqual(body["processing_owner"], {
            "job_id": owner.id,
            "status": owner.status,
            "preview_status": owner.preview_status,
        })
        self.assertIsNotNone(self.db.get_request(106))


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


class TestRoutedCommandDeadlines(unittest.TestCase):
    """Each routed command applies ITS OWN deadline to the real socket.

    The single 15s constant only ever covered enqueue-shaped routes. A
    900s group delete cut off at 15s reports ``api_unavailable``/exit 5
    while the server finishes the destructive work — a production-only
    failure mode, so the constants are pinned at the boundary that
    actually applies them: ``socket.settimeout`` (issue #1063 T3.2).
    """

    def setUp(self) -> None:
        self._temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp_dir.cleanup)
        self.socket_path = os.path.join(self._temp_dir.name, "deadline.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(self.socket_path)
        listener.listen()
        from web.server import ThreadingUnixHTTPServer

        self.server = ThreadingUnixHTTPServer(listener, _JsonEchoHandler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.thread.join, 5)
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)

    def _timeouts_applied(self, run) -> list[float]:
        """Record every ``settimeout`` the real client applies, live."""
        recorded: list[float] = []
        original = socket.socket.settimeout

        def spy(self_socket, value):
            if isinstance(value, float):
                recorded.append(value)
            return original(self_socket, value)

        with patch.object(socket.socket, "settimeout", spy), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            run()
        return recorded

    def _args(self, **values: object) -> argparse.Namespace:
        return argparse.Namespace(
            api_endpoint=api_mutations.UnixApiEndpoint(self.socket_path),
            **values,
        )

    def test_each_command_uses_its_declared_deadline(self) -> None:
        cases = [
            (
                "force-import",
                lambda: pipeline_cli.cmd_force_import(
                    None, self._args(download_log_id=1)),
                api_mutations.TIMEOUT_ENQUEUE_SECONDS,
            ),
            (
                "wrong-match-delete",
                lambda: pipeline_cli.cmd_wrong_match_delete(
                    None, self._args(download_log_id=1, apply=True, json=True)),
                api_mutations.TIMEOUT_SOURCE_DELETE_SECONDS,
            ),
            (
                "wrong-match-delete-group",
                lambda: pipeline_cli.cmd_wrong_match_delete_group(
                    None, self._args(request_id=1, apply=True, json=True)),
                api_mutations.TIMEOUT_GROUP_DELETE_SECONDS,
            ),
            (
                "replace",
                lambda: pipeline_cli.cmd_replace(
                    None,
                    self._args(id=1, target_mb_release_id="x", json=True)),
                api_mutations.TIMEOUT_MIRROR_SECONDS,
            ),
            (
                "merge-rekey",
                lambda: api_mutations.cmd_merge_rekey(
                    None, self._args(request_id=1)),
                api_mutations.TIMEOUT_MIRROR_SECONDS,
            ),
            (
                "beets-distance",
                lambda: pipeline_cli.cmd_beets_distance(
                    None, self._args(download_log_id=1, mbid="1", json=True)),
                api_mutations.TIMEOUT_FOLDER_READ_SECONDS,
            ),
            (
                "import-preview",
                lambda: pipeline_cli.cmd_import_preview_from_download_log(
                    None,
                    self._args(
                        download_log_id=1, path=None, values=False,
                        values_json=None, json=True)),
                api_mutations.TIMEOUT_MEASUREMENT_SECONDS,
            ),
        ]
        for name, run, expected in cases:
            with self.subTest(command=name):
                self.assertEqual(self._timeouts_applied(run), [expected])

    def test_the_deadlines_are_not_all_the_enqueue_default(self) -> None:
        """Known-bad guard: reverting any constant to 15s must be visible."""
        self.assertEqual(api_mutations.TIMEOUT_ENQUEUE_SECONDS, 15.0)
        for constant in (
            api_mutations.TIMEOUT_SOURCE_DELETE_SECONDS,
            api_mutations.TIMEOUT_GROUP_DELETE_SECONDS,
            api_mutations.TIMEOUT_MIRROR_SECONDS,
            api_mutations.TIMEOUT_MEASUREMENT_SECONDS,
            api_mutations.TIMEOUT_FOLDER_READ_SECONDS,
        ):
            self.assertGreater(constant, api_mutations.TIMEOUT_ENQUEUE_SECONDS)

    def test_triage_start_and_poll_use_their_own_deadlines(self) -> None:
        recorded = self._timeouts_applied(
            lambda: pipeline_cli.cmd_wrong_match_triage(
                None,
                self._args(apply=True, json=True),
                sleep=lambda _seconds: None,
            ),
        )
        # Start (enqueue-shaped) then one status poll; the sweep itself is
        # deliberately unbounded.
        self.assertEqual(
            recorded[:2],
            [
                api_mutations.TIMEOUT_ENQUEUE_SECONDS,
                api_mutations.TIMEOUT_POLL_SECONDS,
            ],
        )

    def test_a_short_deadline_really_aborts_the_request(self) -> None:
        """The mechanism is real, not just a number passed around."""
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stalled_path = os.path.join(self._temp_dir.name, "stalled.sock")
        listener.bind(stalled_path)
        listener.listen()

        def serve() -> None:
            connection, _address = listener.accept()
            try:
                threading.Event().wait(2.0)
            finally:
                connection.close()
                listener.close()

        thread = threading.Thread(target=serve)
        thread.start()
        stderr = io.StringIO()
        with redirect_stderr(stderr):
            code = api_mutations._relay(
                api_mutations.UnixApiEndpoint(stalled_path),
                api_mutations._ApiMutation(
                    path="/api/pipeline/upgrade", body={"mb_release_id": "r"}),
                timeout_seconds=0.05,
            )
        thread.join(timeout=5)
        self.assertEqual(code, 5)
        self.assertEqual(json.loads(stderr.getvalue())["error"], "api_unavailable")


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
