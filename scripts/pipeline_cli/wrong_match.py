"""pipeline-cli Wrong Matches queue commands (#495 carve).

``wrong-match-triage`` (whole-queue evidence cleanup), ``wrong-match-delete``
(single source folder), ``wrong-match-delete-group`` (all visible source
folders for one request).
"""

import argparse
import json
import sys


def cmd_wrong_match_triage(db, args):
    """Run evidence-only cleanup for the full Wrong Matches queue."""
    from lib.wrong_match_cleanup_service import (
        OUTCOME_KEYS,
        cleanup_all_wrong_matches,
    )

    forbidden_scope: list[str] = []
    for name in ("download_log_id", "request_id", "limit", "all"):
        value = getattr(args, name, None)
        if value is not None and value is not False:
            forbidden_scope.append(f"--{name.replace('_', '-')}")
    if forbidden_scope:
        print(
            "  wrong-match-triage processes the whole Wrong Matches queue; "
            f"scope flags are not supported: {', '.join(forbidden_scope)}.",
            file=sys.stderr,
        )
        return 2
    if not args.apply:
        print(
            "  Refusing destructive wrong-match triage without --apply. "
            "This command processes the whole Wrong Matches queue.",
            file=sys.stderr,
        )
        return 2

    summary = cleanup_all_wrong_matches(db, confirm_all_wrong_matches=True)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return 0

    for result in summary.results:
        print(
            f"  [{result.download_log_id}] {result.outcome}"
            f"{': ' + result.reason if result.reason else ''}"
        )
    if summary.results:
        print("")
    for outcome in OUTCOME_KEYS:
        print(f"  {outcome}: {getattr(summary, outcome)}")
    print(f"  total: {summary.processed}")
    return 0


def _print_wrong_match_delete_result(result, *, json_output: bool) -> None:
    if json_output:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return
    print(f"  [{result.download_log_id}] {result.outcome}")
    if result.reason:
        print(f"  reason: {result.reason}")
    if result.deleted_path:
        print(f"  deleted_path: {result.deleted_path}")
    if result.path_missing:
        print("  path_missing: yes")
    print(f"  cleared_rows: {result.cleared_rows}")


def cmd_wrong_match_delete(db, args):
    """Delete one visible Wrong Matches source folder."""
    from lib.wrong_match_delete_service import (
        OUTCOME_DELETE_FAILED,
        OUTCOME_DELETED,
        OUTCOME_SKIPPED_ACTIVE_JOB,
        OUTCOME_SKIPPED_INVALID_ROW,
        OUTCOME_SKIPPED_LOCKED,
        OUTCOME_SKIPPED_NOT_VISIBLE,
        OUTCOME_SKIPPED_UNSAFE_PATH,
        delete_wrong_match,
    )

    if not args.apply:
        print(
            "  Refusing destructive wrong-match delete without --apply.",
            file=sys.stderr,
        )
        return 2

    result = delete_wrong_match(
        db,
        args.download_log_id,
        require_visible=True,
    )
    _print_wrong_match_delete_result(result, json_output=args.json)
    if result.outcome == OUTCOME_DELETED:
        return 0
    if result.outcome in (OUTCOME_SKIPPED_INVALID_ROW, OUTCOME_SKIPPED_NOT_VISIBLE):
        return 2
    if result.outcome == OUTCOME_SKIPPED_ACTIVE_JOB:
        return 4
    if result.outcome == OUTCOME_SKIPPED_UNSAFE_PATH:
        return 3
    if result.outcome == OUTCOME_SKIPPED_LOCKED:
        return 5
    if result.outcome == OUTCOME_DELETE_FAILED:
        return 1
    return 1


def cmd_wrong_match_delete_group(db, args):
    """Delete every visible Wrong Matches source folder for one request."""
    from lib.wrong_match_delete_service import delete_wrong_match_group

    if not args.apply:
        print(
            "  Refusing destructive wrong-match group delete without --apply.",
            file=sys.stderr,
        )
        return 2

    summary = delete_wrong_match_group(db, args.request_id)
    if args.json:
        print(json.dumps(summary.to_dict(), indent=2, sort_keys=True))
        return _wrong_match_delete_group_exit_code(summary)

    for result in summary.results:
        print(f"  [{result.download_log_id}] {result.outcome}")
        if result.reason:
            print(f"    reason: {result.reason}")
    if summary.results:
        print("")
    print(f"  deleted: {summary.deleted}")
    print(f"  deleted_paths: {summary.deleted_paths}")
    print(f"  cleared: {summary.cleared}")
    print(f"  skipped: {summary.skipped}")
    print(f"  errors: {summary.errors}")
    print(f"  remaining: {summary.remaining}")
    return _wrong_match_delete_group_exit_code(summary)


def _wrong_match_delete_group_exit_code(summary) -> int:
    from lib.wrong_match_delete_service import (
        OUTCOME_DELETE_FAILED,
        OUTCOME_SKIPPED_ACTIVE_JOB,
        OUTCOME_SKIPPED_INVALID_ROW,
        OUTCOME_SKIPPED_LOCKED,
        OUTCOME_SKIPPED_NOT_VISIBLE,
        OUTCOME_SKIPPED_UNSAFE_PATH,
    )

    if summary.success:
        return 0
    outcomes = {result.outcome for result in summary.results}
    if OUTCOME_DELETE_FAILED in outcomes:
        return 1
    if OUTCOME_SKIPPED_LOCKED in outcomes:
        return 5
    if OUTCOME_SKIPPED_ACTIVE_JOB in outcomes:
        return 4
    if OUTCOME_SKIPPED_UNSAFE_PATH in outcomes:
        return 3
    if outcomes & {OUTCOME_SKIPPED_INVALID_ROW, OUTCOME_SKIPPED_NOT_VISIBLE}:
        return 2
    return 1


def add_wrong_match_subparsers(sub: argparse._SubParsersAction) -> None:
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
