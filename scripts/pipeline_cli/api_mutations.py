"""HTTP-backed CLI adapters for canonical web mutation routes (CD-QUAL-01).

These commands deliberately have no database or mirror setup path: the web
route remains the one execution authority. The decoded relay centralizes
transport and protocol failures while allowing a command to retain its stable
renderer and explicit outcome-to-exit contract.
"""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import dataclass

import msgspec

from web.request_security import CHANNEL_HEADER, CLI_CHANNEL

DEFAULT_API_BASE = "http://127.0.0.1:8085"
_TIMEOUT_SECONDS = 15.0
_INTERNAL_HOST = "cratedigger.internal"


@dataclass(frozen=True)
class TcpApiEndpoint:
    """Explicit standalone-development TCP API origin."""

    api_base: str


@dataclass(frozen=True)
class UnixApiEndpoint:
    """Permissioned local API socket selected by the installed wrapper."""

    socket_path: str


ApiEndpoint = TcpApiEndpoint | UnixApiEndpoint


class _ApiMutation(msgspec.Struct, frozen=True):
    path: str
    body: dict[str, object]


class _ApiResult(msgspec.Struct, frozen=True):
    status: int
    body: bytes


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Return the original redirect response; never replay a mutation."""

    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: object,
        code: int,
        msg: str,
        headers: object,
        newurl: str,
    ) -> None:
        return None


class _UnixHTTPConnection(http.client.HTTPConnection):
    """Stdlib HTTP/1.1 connection whose transport is AF_UNIX."""

    def __init__(self, socket_path: str, *, timeout: float) -> None:
        super().__init__(_INTERNAL_HOST, timeout=timeout)
        self._socket_path = socket_path

    def connect(self) -> None:
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self.timeout)
        try:
            connection.connect(self._socket_path)
        except OSError:
            connection.close()
            raise
        self.sock = connection


def _failure(error: str, detail: str) -> int:
    print(json.dumps({"error": error, "detail": detail}), file=sys.stderr)
    return 5


def _exit_code(status: int) -> int:
    if 200 <= status < 300:
        return 0
    if status == 404:
        return 2
    if status in (400, 422):
        return 3
    if status == 409:
        return 4
    return 5


def _post_unix(
    endpoint: UnixApiEndpoint,
    mutation: _ApiMutation,
    *,
    timeout_seconds: float,
) -> _ApiResult:
    connection = _UnixHTTPConnection(
        endpoint.socket_path,
        timeout=timeout_seconds,
    )
    try:
        connection.request(
            "POST",
            mutation.path,
            body=msgspec.json.encode(mutation.body),
            headers={
                "Host": _INTERNAL_HOST,
                "Content-Type": "application/json",
                CHANNEL_HEADER: CLI_CHANNEL,
            },
        )
        response = connection.getresponse()
        return _ApiResult(status=response.status, body=response.read())
    finally:
        connection.close()


def _post(
    endpoint: ApiEndpoint,
    mutation: _ApiMutation,
    *,
    timeout_seconds: float = _TIMEOUT_SECONDS,
) -> _ApiResult | None:
    try:
        if isinstance(endpoint, UnixApiEndpoint):
            return _post_unix(
                endpoint,
                mutation,
                timeout_seconds=timeout_seconds,
            )
        request = urllib.request.Request(
            f"{endpoint.api_base.rstrip('/')}{mutation.path}",
            data=msgspec.json.encode(mutation.body),
            headers={
                "Content-Type": "application/json",
                CHANNEL_HEADER: CLI_CHANNEL,
            },
            method="POST",
        )
        opener = urllib.request.build_opener(_NoRedirectHandler())
        with opener.open(request, timeout=timeout_seconds) as response:
            return _ApiResult(status=response.status, body=response.read())
    except urllib.error.HTTPError as exc:
        with exc:
            return _ApiResult(status=exc.code, body=exc.read())
    except ValueError as exc:
        _failure("api_protocol_error", str(exc))
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        _failure("api_unavailable", str(exc))
        return None
    except http.client.HTTPException as exc:
        _failure("api_protocol_error", str(exc))
        return None


def _relay_decoded[T](
    endpoint: ApiEndpoint,
    mutation: _ApiMutation,
    *,
    decoder: Callable[[int, bytes], T],
    renderer: Callable[[T], str],
    exit_code_for: Callable[[int, T], int | None] | None = None,
    timeout_seconds: float = _TIMEOUT_SECONDS,
) -> int:
    result = _post(
        endpoint,
        mutation,
        timeout_seconds=timeout_seconds,
    )
    if result is None:
        return 5
    try:
        value = decoder(result.status, result.body)
    except (msgspec.DecodeError, msgspec.ValidationError):
        return _failure(
            "api_protocol_error",
            "API response did not match the expected JSON schema",
        )
    print(renderer(value))
    if exit_code_for is not None:
        explicit_exit_code = exit_code_for(result.status, value)
        if explicit_exit_code is not None:
            return explicit_exit_code
    return _exit_code(result.status)


def _decode_json_object(_status: int, body: bytes) -> dict[str, object]:
    return msgspec.json.decode(body, type=dict[str, object])


def _render_json_object(value: dict[str, object]) -> str:
    return msgspec.json.encode(value).decode()


def _relay(
    endpoint: ApiEndpoint,
    mutation: _ApiMutation,
    *,
    timeout_seconds: float = _TIMEOUT_SECONDS,
) -> int:
    return _relay_decoded(
        endpoint,
        mutation,
        decoder=_decode_json_object,
        renderer=_render_json_object,
        timeout_seconds=timeout_seconds,
    )


def cmd_pipeline_delete(_db: object, args: argparse.Namespace) -> int:
    if args.confirm != "DELETE":
        print(json.dumps({"error": "confirmation_required", "confirm": "DELETE"}),
              file=sys.stderr)
        return 3
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/pipeline/delete", body={"id": args.request_id},
    ))


def cmd_set_quality(_db: object, args: argparse.Namespace) -> int:
    body: dict[str, object] = {"mb_release_id": args.release_id}
    if args.status is not None:
        body["status"] = args.status
    if args.min_bitrate is not None:
        body["min_bitrate"] = args.min_bitrate
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/pipeline/set-quality", body=body,
    ))


def cmd_upgrade(_db: object, args: argparse.Namespace) -> int:
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/pipeline/upgrade", body={"mb_release_id": args.release_id},
    ))


def cmd_wrong_match_converge(_db: object, args: argparse.Namespace) -> int:
    if not args.apply:
        print(json.dumps({"error": "confirmation_required", "flag": "--apply"}),
              file=sys.stderr)
        return 3
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/wrong-matches/converge",
        body={"request_id": args.request_id, "threshold_milli": args.threshold_milli},
    ))


def cmd_resolve_rg(_db: object, args: argparse.Namespace) -> int:
    return _relay(args.api_endpoint, _ApiMutation(
        path=f"/api/pipeline/{args.request_id}/resolve-rg", body={},
    ))


def add_api_mutation_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add CLI-over-HTTP mutation adapters; routes own validation."""
    delete = sub.add_parser("pipeline-delete", help="Delete a pipeline request via the web API")
    delete.add_argument("request_id", type=int)
    delete.add_argument("--confirm", default=None)

    quality = sub.add_parser("set-quality", help="Set request quality via the web API")
    quality.add_argument("release_id")
    quality.add_argument("--status", default=None)
    quality.add_argument("--min-bitrate", type=int, default=None)

    upgrade = sub.add_parser("upgrade", help="Queue a release upgrade via the web API")
    upgrade.add_argument("release_id")

    converge = sub.add_parser("wrong-match-converge", help="Converge one Wrong Matches request via the web API")
    converge.add_argument("request_id", type=int)
    converge.add_argument("threshold_milli", type=int)
    converge.add_argument("--apply", action="store_true")

    resolve = sub.add_parser("resolve-rg", help="Resolve one request release-group via the web API")
    resolve.add_argument("request_id", type=int)
