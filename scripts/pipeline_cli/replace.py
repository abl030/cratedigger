"""pipeline-cli ``replace`` command (#495 carve).

Supersede a request with a new row at a different release id in the
same release group / master. Counterpart of the Replace web action.

Replace's post-supersede cleanup deletes the old request's Wrong Matches
source folders under the private ``0700`` processing tree, so the command
executes through the canonical web route over the permissioned Unix
socket (issue #1063). Run in the operator's own process it would classify
every unreadable folder as missing, clear every pointer, observe zero
remaining, and report success with no warning.
"""

from __future__ import annotations

import argparse

from lib.json_narrow import is_object_list, is_str_object_dict
from scripts.pipeline_cli.api_mutations import (
    TIMEOUT_MIRROR_SECONDS,
    _ApiMutation,
    relay_rendered,
    render_api_error,
)


def _render_replace(status: int, payload: dict[str, object]) -> None:
    if payload.get("outcome") is None:
        render_api_error(status, payload)
        return
    print(f"  Request ID:        {payload.get('request_id')}")
    print(f"  Outcome:           {payload.get('outcome')}")
    if payload.get("new_request_id") is not None:
        print(f"  New request id:    {payload['new_request_id']}")
    if payload.get("current_status") is not None:
        print(f"  Holder status:     {payload['current_status']}")
    if payload.get("descendant_request_id") is not None:
        print(f"  Descendant id:     {payload['descendant_request_id']}")
    if payload.get("reason") is not None:
        print(f"  Reason:            {payload['reason']}")
    owner = payload.get("processing_owner")
    if is_str_object_dict(owner):
        print(
            "  Processing owner:  "
            f"job {owner.get('job_id')} "
            f"({owner.get('status')}/{owner.get('preview_status')})"
        )
    if payload.get("error_message"):
        print(f"  Error message:     {payload['error_message']}")
    warnings = payload.get("warnings")
    if is_object_list(warnings) and warnings:
        print("  Warnings:")
        for warning in warnings:
            print(f"    - {warning}")


def cmd_replace(_db: object, args: argparse.Namespace) -> int:
    """Supersede a request with a new row at a different release id (an
    MB release UUID or a Discogs numeric release id — must share the
    source's pathway and release group/master).

    Thin adapter over ``POST /api/pipeline/<id>/replace``, which is the
    one execution path for both surfaces (see ``CLAUDE.md`` § "CLI ⇄ API
    surface symmetry").

    Exit codes, derived from that route's status codes:
      * 0 — 200 ``RESULT_REPLACED``
      * 2 — 404 ``RESULT_NOT_FOUND``
      * 3 — 422 ``RESULT_TARGET_INVALID`` (``reason`` carries the typed
            sub-code — see ``lib/replace_status.py``),
            ``RESULT_TARGET_RELEASE_GROUP_MISMATCH``,
            ``RESULT_TARGET_SAME_AS_CURRENT``
      * 4 — 409 ``RESULT_WRONG_STATE`` (including supersede race —
            double-click landed first; descendant_request_id is set),
            ``RESULT_TARGET_COLLISION_REQUEST``
      * 5 — 503 ``RESULT_TRANSIENT`` (retryable; mirror unreachable etc.),
            ``RESULT_MIRROR_UNCONFIGURED`` (Discogs mirror not configured)
    """
    return relay_rendered(
        args.api_endpoint,
        _ApiMutation(
            path=f"/api/pipeline/{int(args.id)}/replace",
            body={"target_mb_release_id": args.target_mb_release_id},
        ),
        render=_render_replace,
        json_output=getattr(args, "json", False),
        timeout_seconds=TIMEOUT_MIRROR_SECONDS,
    )


def add_replace_subparser(
    sub: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
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
