"""pipeline-cli Wrong Matches queue commands (#495 carve).

``wrong-match-triage`` (whole-queue evidence cleanup), ``wrong-match-delete``
(single source folder), ``wrong-match-delete-group`` (all visible source
folders for one request).

All three touch the private ``0700`` processing tree, so all three execute
through the canonical web routes over the permissioned Unix socket rather
than in the operator's own process (issue #1063). There is no direct-DB
fallback: the installed CLI cannot traverse that tree, and an in-process
run there reported intact 445MB folders as "missing" and cleared their
pointers. Only the presentation below is CLI-local.
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
        "deleted", "deleted_paths", "cleared", "skipped", "errors", "remaining",
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
    """
    if not args.apply:
        print(
            "  Refusing destructive wrong-match triage without --apply. "
            "This command processes the whole Wrong Matches queue.",
            file=sys.stderr,
        )
        return 2

    return relay_polled(
        args.api_endpoint,
        _ApiMutation(
            path="/api/wrong-matches/triage",
            body={"confirm_all_wrong_matches": True},
        ),
        _ApiMutation(
            path="/api/wrong-matches/triage/status",
            body={},
            method="GET",
        ),
        is_complete=_triage_is_complete,
        render=_render_wrong_match_triage,
        json_output=args.json,
        completed_exit_code=_triage_exit_code,
        sleep=sleep,
    )


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
    """Add ``wrong-match-triage`` / ``wrong-match-delete`` /
    ``wrong-match-delete-group`` (#521 carve out of
    ``routes_meta._build_parser``, verbatim argument definitions)."""
    # wrong-match-triage
    p_triage = sub.add_parser(
        "wrong-match-triage",
        help="Clean the full Wrong Matches queue using existing evidence",
    )
    p_triage.add_argument("--apply", action="store_true",
                          help="Allow destructive full-queue cleanup")
    p_triage.add_argument("--json", action="store_true")

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
