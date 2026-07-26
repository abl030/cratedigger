"""HTTP-backed CLI adapters for canonical web mutation routes (CD-QUAL-01).

These commands deliberately have no database or mirror setup path: the web
route remains the one execution authority and this module only preserves its
JSON response plus the CLI's stable exit-code convention.
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import urllib.error
import urllib.request

import msgspec


DEFAULT_API_BASE = "http://127.0.0.1:8090"
_TIMEOUT_SECONDS = 15.0


class _ApiMutation(msgspec.Struct, frozen=True):
    path: str
    body: dict[str, object]


class _ApiResult(msgspec.Struct, frozen=True):
    status: int
    body: bytes


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


def _post(api_base: str, mutation: _ApiMutation) -> _ApiResult | None:
    request = urllib.request.Request(
        f"{api_base.rstrip('/')}{mutation.path}",
        data=msgspec.json.encode(mutation.body),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_SECONDS) as response:
            return _ApiResult(status=response.status, body=response.read())
    except urllib.error.HTTPError as exc:
        with exc:
            return _ApiResult(status=exc.code, body=exc.read())
    except (urllib.error.URLError, TimeoutError, socket.timeout, OSError) as exc:
        _failure("api_unavailable", str(exc))
        return None


def _relay(api_base: str, mutation: _ApiMutation) -> int:
    result = _post(api_base, mutation)
    if result is None:
        return 5
    try:
        msgspec.json.decode(result.body, type=dict[str, object])
    except (msgspec.DecodeError, msgspec.ValidationError):
        return _failure("api_protocol_error", "API response was not a JSON object")
    print(result.body.decode("utf-8"))
    return _exit_code(result.status)


def cmd_pipeline_delete(_db: object, args: argparse.Namespace) -> int:
    if args.confirm != "DELETE":
        print(json.dumps({"error": "confirmation_required", "confirm": "DELETE"}),
              file=sys.stderr)
        return 3
    return _relay(args.api_base, _ApiMutation(
        path="/api/pipeline/delete", body={"id": args.request_id},
    ))


def cmd_set_quality(_db: object, args: argparse.Namespace) -> int:
    body: dict[str, object] = {"mb_release_id": args.release_id}
    if args.status is not None:
        body["status"] = args.status
    if args.min_bitrate is not None:
        body["min_bitrate"] = args.min_bitrate
    return _relay(args.api_base, _ApiMutation(
        path="/api/pipeline/set-quality", body=body,
    ))


def cmd_upgrade(_db: object, args: argparse.Namespace) -> int:
    return _relay(args.api_base, _ApiMutation(
        path="/api/pipeline/upgrade", body={"mb_release_id": args.release_id},
    ))


def cmd_wrong_match_converge(_db: object, args: argparse.Namespace) -> int:
    if not args.apply:
        print(json.dumps({"error": "confirmation_required", "flag": "--apply"}),
              file=sys.stderr)
        return 3
    return _relay(args.api_base, _ApiMutation(
        path="/api/wrong-matches/converge",
        body={"request_id": args.request_id, "threshold_milli": args.threshold_milli},
    ))


def cmd_resolve_rg(_db: object, args: argparse.Namespace) -> int:
    return _relay(args.api_base, _ApiMutation(
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
