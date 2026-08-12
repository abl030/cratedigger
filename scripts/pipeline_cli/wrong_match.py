"""pipeline-cli Wrong Matches queue commands (#495 carve).

``wrong-match-triage`` (whole-queue evidence cleanup), ``wrong-match-delete``
(single source folder), ``wrong-match-delete-group`` (all visible source
folders for one request), ``wrong-match-triage-cancel`` (request
cancellation of an in-flight sweep, issue #1083).

The first three touch the private ``0700`` processing tree, so all four
execute through the canonical web routes over the permissioned Unix socket
rather than in the operator's own process (issue #1063). There is no
direct-DB fallback: the installed CLI cannot traverse that tree, and an
in-process run there reported intact 445MB folders as "missing" and
cleared their pointers. ``wrong-match-triage-cancel`` doesn't touch that
tree itself, but the sweep it stops runs on a background thread inside
``cratedigger-web`` — the only way to reach it is the same socket. Only
the presentation below is CLI-local.
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable, Mapping

from lib.json_narrow import is_object_list, is_str_object_dict
from scripts.pipeline_cli.api_mutations import (
    TIMEOUT_GROUP_DELETE_SECONDS,
    TIMEOUT_SOURCE_DELETE_SECONDS,
    _ApiMutation,
    _post,
    _relay,
    poll_to_completion,
    relay_polled,
    relay_rendered,
    render_api_error,
)

# Every non-success Wrong Matches delete status already had an exit code
# while the command ran in-process; 500 is the one the generic table would
# have changed (delete_failed has always been 1, not 5).
_WRONG_MATCH_DELETE_EXIT_OVERRIDES: Mapping[int, int] = {500: 1}

def _render_wrong_match_delete(status: int, payload: dict[str, object]) -> None:
    # The route returns the whole typed result on refusals too, so the
    # text output stays identical to the in-process command's (#1063).
    if payload.get("outcome") is None:
        render_api_error(status, payload)
        return
    print(f"  [{payload.get('download_log_id')}] {payload.get('outcome')}")
    if payload.get("reason"):
        print(f"  reason: {payload['reason']}")
    if payload.get("deleted_path"):
        print(f"  deleted_path: {payload['deleted_path']}")
    if payload.get("path_missing"):
        print("  path_missing: yes")
    print(f"  cleared_rows: {payload.get('cleared_rows', 0)}")


def _render_wrong_match_delete_group(
    status: int, payload: dict[str, object],
) -> None:
    results = payload.get("results")
    if not is_object_list(results):
        render_api_error(status, payload)
        return
    for result in results:
        if not is_str_object_dict(result):
            continue
        print(f"  [{result.get('download_log_id')}] {result.get('outcome')}")
        if result.get("reason"):
            print(f"    reason: {result['reason']}")
    if results:
        print()
    for field in (
        "deleted", "cleared_missing", "deleted_paths", "cleared",
        "unavailable", "skipped", "errors", "remaining",
    ):
        print(f"  {field}: {payload.get(field, 0)}")


def _render_wrong_match_triage(status: int, payload: dict[str, object]) -> None:
    from lib.wrong_match_cleanup_service import OUTCOME_KEYS

    summary = payload.get("summary")
    if not is_str_object_dict(summary):
        # A sweep that crashed, or one that finished under a different
        # caller and left no summary here, is not an "API refused" —
        # say what actually happened (#1063 review T3.5).
        state = payload.get("state")
        if state == "failed":
            print(f"  Wrong Matches sweep FAILED: {payload.get('error')}")
        elif state == "idle":
            print(
                "  No Wrong Matches sweep result is available: the server "
                "reports no sweep in progress and no summary. It may have "
                "been restarted mid-sweep."
            )
        else:
            render_api_error(status, payload)
        return
    if payload.get("state") == "cancelled":
        # Issue #1083: the operator (or the browser's Stop button) cut
        # the sweep short. ``summary`` still holds exactly what ran
        # before the stop — say so distinctly from a full completion.
        print("  Wrong Matches sweep CANCELLED — reporting what ran "
              "before the stop:")
    results = summary.get("results")
    if is_object_list(results):
        for result in results:
            if not is_str_object_dict(result):
                continue
            reason = result.get("reason")
            print(
                f"  [{result.get('download_log_id')}] {result.get('outcome')}"
                f"{': ' + str(reason) if reason else ''}"
            )
        if results:
            print()
    for outcome in OUTCOME_KEYS:
        print(f"  {outcome}: {summary.get(outcome, 0)}")
    print(f"  total: {summary.get('processed', 0)}")


def _triage_is_complete(payload: dict[str, object]) -> bool:
    return payload.get("state") != "running"


def _triage_exit_code(payload: dict[str, object]) -> int:
    if payload.get("state") == "completed" and not payload.get("error"):
        return 0
    return 5


def _triage_status_mutation() -> _ApiMutation:
    return _ApiMutation(
        path="/api/wrong-matches/triage/status", body={}, method="GET",
    )


def cmd_wrong_match_triage(
    _db: object,
    args: argparse.Namespace,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> int:
    """Run evidence-only cleanup for the full Wrong Matches queue.

    Starts the canonical background sweep and follows its status route to
    completion, so the operator still gets the whole summary and exit 0
    while the deletions happen under the service identity (issue #1063).

    ``Ctrl-C`` used to stop the sweep directly, back when it ran
    in-process. Now it only detaches the CLI from a sweep that keeps
    running — UNLESS we catch it here and request cancellation through
    the exact same canonical route the web UI's Stop button posts to
    (issue #1083). No direct call, no direct-DB fallback: the deletions
    happen under the service identity over the permissioned socket
    either way (issue #1063), and cancellation is just one more request
    over that same path. A failed cancel POST (refused socket, timeout,
    or a route-level 500) gets one retry and, if that retry also fails,
    a stderr line saying so before the CLI falls through to following
    the sweep's status — silently swallowing that failure would have
    the CLI claim it stopped the sweep while the whole remaining queue
    keeps deleting underneath it.

    This is the ONE caller in the whole codebase that sends
    ``arm_pending: true`` (issue #1106 F3): we are specifically racing
    OUR OWN start POST just above, so a cancel that lands before the
    server's ``start()`` has flipped the sweep to ``running`` must still
    stop it. Every other cancel caller (the web UI's Stop button, the
    standalone ``wrong-match-triage-cancel`` command below) stays
    unarmed — arming there would risk pre-cancelling a later, unrelated
    sweep that operator never asked to stop.
    """
    if not args.apply:
        print(
            "  Refusing destructive wrong-match triage without --apply. "
            "This command processes the whole Wrong Matches queue.",
            file=sys.stderr,
        )
        return 2

    status_request = _triage_status_mutation()
    try:
        return relay_polled(
            args.api_endpoint,
            _ApiMutation(
                path="/api/wrong-matches/triage",
                body={"confirm_all_wrong_matches": True},
            ),
            status_request,
            is_complete=_triage_is_complete,
            render=_render_wrong_match_triage,
            json_output=args.json,
            completed_exit_code=_triage_exit_code,
            sleep=sleep,
        )
    except KeyboardInterrupt:
        print(
            "\n  Stopping the sweep — waiting for the current row to "
            "finish...",
            file=sys.stderr,
        )
        cancel_request = _ApiMutation(
            path="/api/wrong-matches/triage/cancel",
            body={"arm_pending": True},
        )
        # One retry before giving up (``_post``'s own contract:
        # ``report_failure=False`` silences the structured line for a
        # RETRIED attempt, the caller reports once it finally gives
        # up). ``_post`` only returns ``None`` on a transport failure —
        # a route-level 500 still comes back as a normal non-2xx
        # result — so both are checked explicitly, matching the
        # browser's Stop button, which toasts "Stop request failed" on
        # the same condition (web/js/wrong-matches.js).
        result = _post(args.api_endpoint, cancel_request, report_failure=False)
        if result is None or not 200 <= result.status < 300:
            result = _post(args.api_endpoint, cancel_request)
            if result is None or not 200 <= result.status < 300:
                print(
                    "  Stop request failed — the sweep is still running "
                    "and may delete the rest of the queue; following its "
                    "status anyway.",
                    file=sys.stderr,
                )
        return poll_to_completion(
            args.api_endpoint,
            status_request,
            is_complete=_triage_is_complete,
            render=_render_wrong_match_triage,
            json_output=args.json,
            completed_exit_code=_triage_exit_code,
            sleep=sleep,
        )


def cmd_wrong_match_triage_cancel(_db: object, args: argparse.Namespace) -> int:
    """Request cancellation of the in-flight bulk triage sweep.

    Reaches the exact same canonical route the CLI's own ``Ctrl-C``
    handler (``cmd_wrong_match_triage``, above) and the web UI's Stop
    button use (issue #1083). This is the only way to stop a sweep that
    has no interactive terminal left to catch a signal — one started
    over an SSH session that then dropped (SIGHUP, no ``KeyboardInterrupt``),
    or over any connection the operator is no longer attached to. Always
    exits 0: the route itself is a no-op, never a refusal, whether or not
    a sweep happens to be running.

    Sends no ``arm_pending`` (defaults false, issue #1106 F3): this
    command has no start POST of its own to race, so it must stay a
    pure #1083 no-op when nothing is running — arming here would risk
    silently pre-cancelling a LATER, unrelated sweep the operator has
    not even started yet. The documented recovery sequence (this
    command, then a fresh ``wrong-match-triage --apply``) relies on
    that: the fresh sweep must run normally, not land pre-cancelled.
    """
    return _relay(args.api_endpoint, _ApiMutation(
        path="/api/wrong-matches/triage/cancel", body={},
    ))


def cmd_wrong_match_delete(_db: object, args: argparse.Namespace) -> int:
    """Delete one visible Wrong Matches source folder."""
    if not args.apply:
        print(
            "  Refusing destructive wrong-match delete without --apply.",
            file=sys.stderr,
        )
        return 2

    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path="/api/wrong-matches/delete",
            body={"download_log_id": args.download_log_id},
        ),
        render=_render_wrong_match_delete,
        json_output=args.json,
        timeout_seconds=TIMEOUT_SOURCE_DELETE_SECONDS,
        exit_overrides=_WRONG_MATCH_DELETE_EXIT_OVERRIDES,
    )


def cmd_wrong_match_delete_group(_db: object, args: argparse.Namespace) -> int:
    """Delete every visible Wrong Matches source folder for one request."""
    if not args.apply:
        print(
            "  Refusing destructive wrong-match group delete without --apply.",
            file=sys.stderr,
        )
        return 2

    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path="/api/wrong-matches/delete-group",
            body={"request_id": args.request_id},
        ),
        render=_render_wrong_match_delete_group,
        json_output=args.json,
        timeout_seconds=TIMEOUT_GROUP_DELETE_SECONDS,
        exit_overrides=_WRONG_MATCH_DELETE_EXIT_OVERRIDES,
    )


def add_wrong_match_subparsers(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Add ``wrong-match-triage`` / ``wrong-match-triage-cancel`` /
    ``wrong-match-delete`` / ``wrong-match-delete-group`` (#521 carve out
    of ``routes_meta._build_parser``, verbatim argument definitions)."""
    # wrong-match-triage
    p_triage = sub.add_parser(
        "wrong-match-triage",
        help="Clean the full Wrong Matches queue using existing evidence",
    )
    p_triage.add_argument("--apply", action="store_true",
                          help="Allow destructive full-queue cleanup")
    p_triage.add_argument("--json", action="store_true")

    # wrong-match-triage-cancel
    sub.add_parser(
        "wrong-match-triage-cancel",
        help="Request cancellation of the in-flight Wrong Matches "
             "triage sweep, if any",
    )

    # wrong-match-delete
    p_wm_delete = sub.add_parser(
        "wrong-match-delete",
        help="Delete one visible Wrong Matches source folder",
    )
    p_wm_delete.add_argument("download_log_id", type=int)
    p_wm_delete.add_argument("--apply", action="store_true",
                             help="Allow destructive source deletion")
    p_wm_delete.add_argument("--json", action="store_true")

    # wrong-match-delete-group
    p_wm_delete_group = sub.add_parser(
        "wrong-match-delete-group",
        help="Delete visible Wrong Matches source folders for one request",
    )
    p_wm_delete_group.add_argument("request_id", type=int)
    p_wm_delete_group.add_argument("--apply", action="store_true",
                                   help="Allow destructive source deletion")
    p_wm_delete_group.add_argument("--json", action="store_true")
