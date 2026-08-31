"""HTTP-backed CLI adapters for canonical web mutation routes (CD-QUAL-01).

These commands deliberately have no database or mirror setup path: the web
route remains the one execution authority and this module only preserves its
JSON response plus the CLI's stable exit-code convention.

Issue #1063 widened the set: every action that touches a protected
quarantine path now runs here too, because the installed CLI executes as
the invoking operator while the private ``0700`` processing tree is
readable only by the service identity. Routing them through the
permissioned Unix socket makes ONE identity own those filesystem facts.
"""

from __future__ import annotations

import argparse
import http.client
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass

import msgspec

from lib.surface_outcomes import exit_code_for_http_status
from web.request_security import CHANNEL_HEADER, CLI_CHANNEL

DEFAULT_API_BASE = "http://127.0.0.1:8085"
_TIMEOUT_SECONDS = 15.0
_INTERNAL_HOST = "cratedigger.internal"

# Per-command deadlines. The historical single 15s constant only ever
# covered enqueue-shaped routes; the #1063 commands run real work inside
# the request, and one blanket value would either time out honest work or
# hide a wedged socket. Each value below is the work the route performs.
TIMEOUT_ENQUEUE_SECONDS = _TIMEOUT_SECONDS
"""Row lookups plus one INSERT (delete/upgrade/force-import/resolve-rg)."""

TIMEOUT_MIRROR_SECONDS = 300.0
"""Replace: several inline MB/Discogs mirror lookups (this deployment's
mirror timeout is 15s per call), a Beets exact delete, Wrong Matches group
cleanup, and staging cleanup — all before the response is written.
Also merge-rekey (#1089): one inline MusicBrainz merge-survivor lookup at
the same 15s mirror timeout, plus Beets reads, PostgreSQL work, and — when
the request links current evidence, which is now mandatory (#1089 MAJOR-C,
review round 3) — a per-file walk of the survivor album over virtiofs to
compute its fresh content fingerprint (the evidence-lineage witness) — see
``cmd_merge_rekey``."""

TIMEOUT_TAG_SYNC_SECONDS = 660.0
"""sync-file-tags (#1260): the route's worst case is two per-file tag
scans over virtiofs plus a ``beet write`` bounded at
``lib.beets_tag_sync.TAG_SYNC_TIMEOUT_SECONDS`` (300s). The client budget
must strictly exceed that route-side worst case so a slow-but-successful
write still returns its verdict instead of a client timeout (#1260 review
F7): 2 × the write bound + 60s of scan/transport slack."""

TIMEOUT_SOURCE_DELETE_SECONDS = 300.0
"""One ``rmtree`` of a full album folder over virtiofs, behind an
advisory lock that may be held by a concurrent cleanup."""

TIMEOUT_GROUP_DELETE_SECONDS = 900.0
"""The same, once per visible Wrong Matches candidate for a request."""

TIMEOUT_MEASUREMENT_SECONDS = 900.0
"""Download-log import preview: a full private snapshot copy of the album
plus audio validation and spectral measurement, inline in the request."""

TIMEOUT_FOLDER_READ_SECONDS = 180.0
"""Beets distance: tag reads across every file in the folder plus one MB
or Discogs mirror lookup."""

TIMEOUT_QUARANTINE_SCAN_SECONDS = 60.0
"""Triage quarantine: one ``get_wrong_matches`` projection plus four
immediate-children ``scandir``/``stat`` walks (download-dir-rooted and
processing-side ``failed_imports``/``wrong_matches``) over virtiofs — no
writes and no lock contention, but each root can hold many accumulated
folders, so this stays above the plain enqueue deadline."""

TIMEOUT_POLL_SECONDS = 30.0
"""One bulk-triage status poll; the sweep itself is unbounded by design."""

_TRIAGE_POLL_INTERVAL_SECONDS = 2.0

#: A transient blip on one cheap status read must not abandon the
#: operator mid-sweep while the destructive work continues server-side.
_TRIAGE_POLL_MAX_CONSECUTIVE_FAILURES = 5


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
    method: str = "POST"
    """``GET`` reads (beets distance, triage status) share this transport
    so there is exactly one socket/TCP client, one redirect policy, and
    one failure vocabulary."""


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


def _send_unix(
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
        headers = {
            "Host": _INTERNAL_HOST,
            CHANNEL_HEADER: CLI_CHANNEL,
        }
        body: bytes | None = None
        if mutation.method != "GET":
            body = msgspec.json.encode(mutation.body)
            headers["Content-Type"] = "application/json"
        connection.request(
            mutation.method,
            mutation.path,
            body=body,
            headers=headers,
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
    report_failure: bool = True,
) -> _ApiResult | None:
    """``report_failure=False`` silences the structured stderr line for a
    RETRIED attempt; the caller reports once when it finally gives up."""
    try:
        if isinstance(endpoint, UnixApiEndpoint):
            return _send_unix(
                endpoint,
                mutation,
                timeout_seconds=timeout_seconds,
            )
        headers = {CHANNEL_HEADER: CLI_CHANNEL}
        data: bytes | None = None
        if mutation.method != "GET":
            data = msgspec.json.encode(mutation.body)
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            f"{endpoint.api_base.rstrip('/')}{mutation.path}",
            data=data,
            headers=headers,
            method=mutation.method,
        )
        opener = urllib.request.build_opener(_NoRedirectHandler())
        with opener.open(request, timeout=timeout_seconds) as response:
            return _ApiResult(status=response.status, body=response.read())
    except urllib.error.HTTPError as exc:
        with exc:
            return _ApiResult(status=exc.code, body=exc.read())
    except ValueError as exc:
        _report(report_failure, "api_protocol_error", str(exc))
        return None
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        _report(report_failure, "api_unavailable", str(exc))
        return None
    except http.client.HTTPException as exc:
        _report(report_failure, "api_protocol_error", str(exc))
        return None


def _report(enabled: bool, error: str, detail: str) -> None:
    if enabled:
        _failure(error, detail)


def _decode(
    result: _ApiResult, *, report: bool = True,
) -> dict[str, object] | None:
    try:
        return msgspec.json.decode(result.body, type=dict[str, object])
    except (msgspec.DecodeError, msgspec.ValidationError):
        _report(report, "api_protocol_error",
                "API response was not a JSON object")
        return None


def _relay(
    endpoint: ApiEndpoint,
    mutation: _ApiMutation,
    *,
    timeout_seconds: float = _TIMEOUT_SECONDS,
    exit_overrides: Mapping[int, int] | None = None,
) -> int:
    result = _post(
        endpoint,
        mutation,
        timeout_seconds=timeout_seconds,
    )
    if result is None:
        return 5
    if _decode(result) is None:
        return 5
    print(result.body.decode("utf-8"))
    return exit_code_for_http_status(result.status, exit_overrides)


ApiRenderer = Callable[[int, dict[str, object]], None]


def relay_rendered(
    endpoint: ApiEndpoint,
    mutation: _ApiMutation,
    *,
    render: ApiRenderer,
    json_output: bool,
    timeout_seconds: float = _TIMEOUT_SECONDS,
    exit_overrides: Mapping[int, int] | None = None,
) -> int:
    """Relay one canonical route and keep the command's own presentation.

    The route stays the single execution authority; only rendering is
    CLI-local, so an operator keeps the text output (and the ``--json``
    flag) these commands have always had.
    """
    result = _post(endpoint, mutation, timeout_seconds=timeout_seconds)
    if result is None:
        return 5
    payload = _decode(result)
    if payload is None:
        return 5
    if json_output:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        render(result.status, payload)
    return exit_code_for_http_status(result.status, exit_overrides)


def poll_to_completion(
    endpoint: ApiEndpoint,
    status_request: _ApiMutation,
    *,
    is_complete: Callable[[dict[str, object]], bool],
    render: ApiRenderer,
    json_output: bool,
    completed_exit_code: Callable[[dict[str, object]], int],
    poll_timeout_seconds: float = TIMEOUT_POLL_SECONDS,
    poll_interval_seconds: float = _TRIAGE_POLL_INTERVAL_SECONDS,
    max_consecutive_poll_failures: int = _TRIAGE_POLL_MAX_CONSECUTIVE_FAILURES,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Follow an already-started async route's status to a terminal state.

    Split out of ``relay_polled`` (issue #1083) so a caller that catches
    ``KeyboardInterrupt`` mid-poll — to send a cancel through the same
    canonical route the web UI's Stop button uses — can resume following
    the SAME status route afterward without re-issuing the start
    mutation (which would either restart nothing useful or 409 against a
    sweep that is already stopping).
    """
    consecutive_failures = 0
    while True:
        sleep(poll_interval_seconds)
        # Only the attempt that gives up reports: retried blips would
        # otherwise print up to five structured failures for one recovery.
        final_attempt = (
            consecutive_failures + 1 >= max_consecutive_poll_failures
        )
        polled = _post(
            endpoint,
            status_request,
            timeout_seconds=poll_timeout_seconds,
            report_failure=final_attempt,
        )
        payload = None if polled is None else _decode(polled, report=final_attempt)
        if polled is None or payload is None:
            consecutive_failures += 1
            if consecutive_failures >= max_consecutive_poll_failures:
                return 5
            continue
        consecutive_failures = 0
        if not 200 <= polled.status < 300:
            if json_output:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                render(polled.status, payload)
            return exit_code_for_http_status(polled.status)
        if is_complete(payload):
            if json_output:
                print(json.dumps(payload, indent=2, sort_keys=True))
            else:
                render(polled.status, payload)
            return completed_exit_code(payload)


def relay_polled(
    endpoint: ApiEndpoint,
    start: _ApiMutation,
    status_request: _ApiMutation,
    *,
    is_complete: Callable[[dict[str, object]], bool],
    render: ApiRenderer,
    json_output: bool,
    completed_exit_code: Callable[[dict[str, object]], int],
    start_timeout_seconds: float = _TIMEOUT_SECONDS,
    poll_timeout_seconds: float = TIMEOUT_POLL_SECONDS,
    poll_interval_seconds: float = _TRIAGE_POLL_INTERVAL_SECONDS,
    max_consecutive_poll_failures: int = _TRIAGE_POLL_MAX_CONSECUTIVE_FAILURES,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Start an asynchronous route, then follow it to completion.

    The bulk Wrong Matches sweep runs on a web thread and answers 202
    immediately; the operator's command has always blocked until the
    sweep finished and then printed its summary. Polling the canonical
    status route keeps that contract without giving the CLI a second
    execution path. The sweep itself is deliberately unbounded — it takes
    minutes when rows need re-measurement — so only each individual poll
    has a deadline.
    """
    started = _post(endpoint, start, timeout_seconds=start_timeout_seconds)
    if started is None:
        return 5
    start_payload = _decode(started)
    if start_payload is None:
        return 5
    if not 200 <= started.status < 300:
        if json_output:
            print(json.dumps(start_payload, indent=2, sort_keys=True))
        else:
            render(started.status, start_payload)
        return exit_code_for_http_status(started.status)

    return poll_to_completion(
        endpoint,
        status_request,
        is_complete=is_complete,
        render=render,
        json_output=json_output,
        completed_exit_code=completed_exit_code,
        poll_timeout_seconds=poll_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        max_consecutive_poll_failures=max_consecutive_poll_failures,
        sleep=sleep,
    )


def render_api_error(status: int, payload: Mapping[str, object]) -> None:
    """Print the route's own refusal; used by every rendered adapter."""
    error = payload.get("error") or payload.get("reason") or payload.get("detail")
    print(f"  API refused ({status}): {error or 'no detail'}")


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


def cmd_merge_rekey(_db: object, args: argparse.Namespace) -> int:
    """Thin HTTP adapter for ``POST /api/pipeline/<id>/merge-rekey`` (#1089).

    The route is the one canonical execution path
    (``MergeRekeyService.rekey_request``); every status this command's
    outcomes can produce (``lib.merge_rekey_service.MERGE_REKEY_HTTP_STATUS``
    — 200/404/409/422/503) already matches
    ``lib.surface_outcomes.exit_code_for_http_status``'s default
    status→exit mapping, so no ``exit_overrides`` are needed.

    Uses ``TIMEOUT_MIRROR_SECONDS``, not the 15s enqueue default: the route
    itself performs an inline MusicBrainz merge-survivor lookup (this
    deployment's own mirror timeout is 15s), Beets reads, PostgreSQL work,
    and — for the now-mandatory evidence-lineage witness (#1089 MAJOR-C,
    review round 3) — a per-file walk of the survivor album over virtiofs to
    compute its fresh content fingerprint, all before it responds. The 15s
    default would time out honest in-flight work and report a failure for a
    mutation that may already have committed.
    """
    return _relay(args.api_endpoint, _ApiMutation(
        path=f"/api/pipeline/{args.request_id}/merge-rekey", body={},
    ), timeout_seconds=TIMEOUT_MIRROR_SECONDS)


def cmd_sync_file_tags(_db: object, args: argparse.Namespace) -> int:
    """Thin HTTP adapter for
    ``POST /api/audit/retag-divergence/album/<id>/sync-tags`` (#1260).

    The route is the one canonical execution path
    (``lib.beets_tag_sync.sync_album_file_tags_from_borrowed_factory``);
    every status its outcomes can produce
    (``lib.beets_tag_sync.TAG_SYNC_HTTP_STATUS`` — 200/404/409/503)
    already matches ``lib.surface_outcomes.exit_code_for_http_status``'s
    default status→exit mapping, so no ``exit_overrides`` are needed.

    The client budget must strictly EXCEED the route's own worst case —
    two per-file tag scans over virtiofs plus a ``beet write`` bounded at
    ``lib.beets_tag_sync.TAG_SYNC_TIMEOUT_SECONDS`` (300s) — or a slow
    write loses the client first and the operator gets a timeout for a
    write that landed, with no verdict (#1260 review F7). Hence a
    dedicated budget rather than ``TIMEOUT_MIRROR_SECONDS`` (which equals
    the write bound exactly).
    """
    return _relay(args.api_endpoint, _ApiMutation(
        path=(
            f"/api/audit/retag-divergence/album/{args.album_id}/sync-tags"
        ),
        body={"expected_mb_albumid": args.expected_mb_albumid},
    ), timeout_seconds=TIMEOUT_TAG_SYNC_SECONDS)


def cmd_library_census_refresh(_db: object, args: argparse.Namespace) -> int:
    """Thin HTTP adapter for
    ``POST /api/pipeline/dashboard/library-census/refresh``.

    The route is the one canonical execution path: it writes the trigger
    file the module's ``cratedigger-library-completeness-census.path``
    unit watches, and the daily census oneshot itself remains the single
    census execution path. Relayed rather than written directly because
    the trigger lives in the web service's state dir
    (``0755 cratedigger``), which the invoking operator cannot write —
    the write-side analogue of the CLI-over-HTTP permission relays like
    ``triage quarantine``.

    Exit codes: 0 — 200 requested; 5 — 503 unconfigured/unwritable, or
    the API unreachable.
    """
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/pipeline/dashboard/library-census/refresh",
        body={},
    ))


def add_api_mutation_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add CLI-over-HTTP mutation adapters; routes own validation."""
    delete = sub.add_parser("pipeline-delete", help="Delete a pipeline request via the web API")
    delete.add_argument("request_id", type=int)
    delete.add_argument("--confirm", default=None)

    sub.add_parser(
        "library-census-refresh",
        help="Request an out-of-schedule library-completeness census run "
             "via the web API (the daily oneshot stays the execution path)",
    )

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

    merge_rekey = sub.add_parser(
        "merge-rekey",
        help="Rekey an imported request onto the MusicBrainz merge "
             "survivor Beets already holds, via the web API. "
             "Request-ledger-only; never mutates Beets.",
    )
    merge_rekey.add_argument("request_id", type=int)

    sync_file_tags = sub.add_parser(
        "sync-file-tags",
        help="Write one Beets album's file tags from its DB identity "
             "(the retag -W divergence heal, #1260), via the web API. "
             "Verified by re-reading the files.",
    )
    sync_file_tags.add_argument(
        "album_id", type=int, help="Beets album id to sync.",
    )
    sync_file_tags.add_argument(
        "expected_mb_albumid",
        help="The DB mb_albumid you observed (compare-and-set: the sync "
             "refuses if the album has since moved).",
    )
