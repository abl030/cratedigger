"""pipeline-cli ``replace`` command (#495 carve).

Supersede a request with a new row at a different release id in the
same release group / master. Counterpart of the Replace web action.
"""

import argparse
import json

from scripts.pipeline_cli._format import _json_default


def cmd_replace(db, args):
    """Supersede a request with a new row at a different release id (an
    MB release UUID or a Discogs numeric release id — must share the
    source's pathway and release group/master).

    Counterpart of ``POST /api/pipeline/<id>/replace``. Both surfaces
    wrap ``MbidReplaceService.replace_request_mbid`` — keep them in
    sync (see ``CLAUDE.md`` § "CLI ⇄ API surface symmetry").

    Exit codes:
      * 0 — ``RESULT_REPLACED``
      * 2 — ``RESULT_NOT_FOUND``
      * 3 — ``RESULT_TARGET_INVALID`` (``reason`` carries the typed
            sub-code — see ``lib/replace_status.py``),
            ``RESULT_TARGET_RELEASE_GROUP_MISMATCH``,
            ``RESULT_TARGET_SAME_AS_CURRENT`` (semantic input violations)
      * 4 — ``RESULT_WRONG_STATE`` (including supersede race —
            double-click landed first; descendant_request_id is set),
            ``RESULT_TARGET_COLLISION_REQUEST``
      * 5 — ``RESULT_TRANSIENT`` (retryable; mirror unreachable etc.),
            ``RESULT_MIRROR_UNCONFIGURED`` (Discogs mirror not configured)
    """
    from lib.config import read_runtime_config
    from lib.mbid_replace_service import (
        MbidReplaceService,
        RESULT_MIRROR_UNCONFIGURED,
        RESULT_NOT_FOUND,
        RESULT_REPLACED,
        RESULT_TARGET_COLLISION_REQUEST,
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
        RESULT_TRANSIENT,
        RESULT_WRONG_STATE,
    )

    cfg = read_runtime_config()
    svc = MbidReplaceService(db=db, config=cfg)
    result = svc.replace_request_mbid(
        int(args.id),
        target_mb_release_id=args.target_mb_release_id,
    )

    payload = {
        "request_id": result.request_id,
        "outcome": result.outcome,
        "new_request_id": result.new_request_id,
        "current_status": result.current_status,
        "descendant_request_id": result.descendant_request_id,
        "error_message": result.error_message,
        "reason": result.reason,
        "warnings": list(result.warnings),
    }
    if getattr(args, "json", False):
        print(json.dumps(payload, indent=2, sort_keys=True,
                         default=_json_default))
    else:
        print(f"  Request ID:        {payload['request_id']}")
        print(f"  Outcome:           {result.outcome}")
        if result.new_request_id is not None:
            print(f"  New request id:    {result.new_request_id}")
        if result.current_status is not None:
            print(f"  Holder status:     {result.current_status}")
        if result.descendant_request_id is not None:
            print(f"  Descendant id:     {result.descendant_request_id}")
        if result.reason is not None:
            print(f"  Reason:            {result.reason}")
        if result.error_message:
            print(f"  Error message:     {result.error_message}")
        if result.warnings:
            print("  Warnings:")
            for w in result.warnings:
                print(f"    - {w}")

    if result.outcome == RESULT_REPLACED:
        return 0
    if result.outcome == RESULT_NOT_FOUND:
        return 2
    if result.outcome in (
        RESULT_TARGET_INVALID,
        RESULT_TARGET_RELEASE_GROUP_MISMATCH,
        RESULT_TARGET_SAME_AS_CURRENT,
    ):
        return 3
    if result.outcome in (
        RESULT_WRONG_STATE,
        RESULT_TARGET_COLLISION_REQUEST,
    ):
        return 4
    if result.outcome in (RESULT_TRANSIENT, RESULT_MIRROR_UNCONFIGURED):
        return 5
    return 1


def add_replace_subparser(sub: argparse._SubParsersAction) -> None:
    """Add ``replace`` (#521 carve out of ``routes_meta._build_parser``,
    verbatim argument definitions)."""
    p_replace = sub.add_parser(
        "replace",
        help="Supersede a request with a new row at a different release id "
             "in the same release group/master (same pathway as the source)")
    p_replace.add_argument("id", type=int, help="Source request ID")
    p_replace.add_argument(
        "--to", dest="target_mb_release_id", required=True,
        help="Target release id — MB UUID or Discogs numeric id; must "
             "share the source's pathway and release group/master")
    p_replace.add_argument("--json", action="store_true",
                           help="Print structured JSON instead of text")
