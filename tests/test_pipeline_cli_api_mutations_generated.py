"""Generated boundary proof for API-backed CLI response handling (CD-QUAL-01)."""

from __future__ import annotations

import io
import os
import socket
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler
from typing import ClassVar, Self
from unittest.mock import patch

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from scripts.pipeline_cli import api_mutations


def _status_has_expected_exit(
    transport: str,
    status: int,
    exit_code: int,
) -> bool:
    """Checker kept separate so its known-bad test proves it can fail.

    This is the DEFAULT table. Per-command ``exit_overrides`` (e.g.
    beets-distance's 410 -> 4, the delete commands' 500 -> 1) are pinned
    against their own commands, never folded in here (issue #1063).
    """
    if transport not in {"tcp", "unix"}:
        return False
    expected = 0 if 200 <= status < 300 else 2 if status == 404 else 3 if status in (400, 422) else 4 if status == 409 else 5
    return exit_code == expected


def _selected_transport_is_fallback_free(
    transport: str,
    tcp_attempts: int,
) -> bool:
    """Unix selection must never acquire a hidden TCP attempt."""
    return transport != "unix" or tcp_attempts == 0


class _Response:
    def __init__(self, status: int) -> None:
        self.status = status

    def read(self) -> bytes:
        return b'{"status":"route"}'

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_values: object) -> None:
        return None


class _UnixStatusHandler(BaseHTTPRequestHandler):
    status: ClassVar[int] = 200

    def do_POST(self) -> None:
        content_length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(content_length)
        body = b'{"status":"route"}'
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *_args: object) -> None:
        return None


class TestApiMutationGenerated(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from web.server import ThreadingUnixHTTPServer

        cls._temp_dir = tempfile.TemporaryDirectory()
        cls.socket_path = os.path.join(cls._temp_dir.name, "generated.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(cls.socket_path)
        listener.listen()
        cls.server = ThreadingUnixHTTPServer(listener, _UnixStatusHandler)
        cls.thread = threading.Thread(
            target=cls.server.serve_forever,
            daemon=True,
        )
        cls.thread.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=5)
        cls._temp_dir.cleanup()

    @given(
        transport=st.sampled_from(["tcp", "unix"]),
        status=st.one_of(
            st.integers(min_value=200, max_value=203),
            st.integers(min_value=300, max_value=399),
            st.integers(min_value=400, max_value=599),
        ),
    )
    def test_real_relay_obeys_status_mapping_on_both_transports(
        self,
        transport: str,
        status: int,
    ) -> None:
        mutation = api_mutations._ApiMutation(
            path="/api/pipeline/upgrade",
            body={"mb_release_id": "r"},
        )
        with patch("sys.stdout", new_callable=io.StringIO):
            if transport == "tcp":
                with patch(
                    "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
                    return_value=_Response(status),
                ):
                    actual = api_mutations._relay(
                        api_mutations.TcpApiEndpoint("http://api"),
                        mutation,
                    )
            else:
                _UnixStatusHandler.status = status
                actual = api_mutations._relay(
                    api_mutations.UnixApiEndpoint(self.socket_path),
                    mutation,
                )
        self.assertTrue(
            _status_has_expected_exit(transport, status, actual),
        )

    def test_known_bad_checker_self_test(self) -> None:
        self.assertFalse(_status_has_expected_exit("unix", 404, 0))
        self.assertFalse(_status_has_expected_exit("unix", 410, 4))
        self.assertTrue(_status_has_expected_exit("unix", 410, 5))
        self.assertFalse(_status_has_expected_exit("unknown", 200, 0))
        self.assertFalse(_selected_transport_is_fallback_free("unix", 1))

    @given(
        socket_leaf=st.text(
            alphabet="abcdefghijklmnopqrstuvwxyz0123456789",
            min_size=1,
            max_size=20,
        ),
    )
    def test_missing_unix_socket_never_falls_back_to_tcp(
        self,
        socket_leaf: str,
    ) -> None:
        mutation = api_mutations._ApiMutation(
            path="/api/pipeline/upgrade",
            body={"mb_release_id": "r"},
        )
        socket_path = os.path.join(
            self._temp_dir.name,
            f"missing-{socket_leaf}.sock",
        )
        with patch(
            "scripts.pipeline_cli.api_mutations.urllib.request.OpenerDirector.open",
        ) as tcp_open, patch(
            "sys.stderr",
            new_callable=io.StringIO,
        ):
            actual = api_mutations._relay(
                api_mutations.UnixApiEndpoint(socket_path),
                mutation,
            )
        self.assertEqual(actual, 5)
        self.assertTrue(
            _selected_transport_is_fallback_free(
                "unix",
                tcp_open.call_count,
            ),
        )
