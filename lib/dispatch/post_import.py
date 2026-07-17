"""Post-import search-policy application.

One owner for what follows a successful retained import: the canonical
decision->(status, override, denylist) resolution, the requeue transition,
peer attribution, and gate staging. Decisions 17-19 make this policy
identical for automatic and operator (force/manual) imports.

``finalize_request`` is the module-local DI seam, same shape as its
siblings (``lib.dispatch.outcome_actions``, ``harness.import_one``).
"""

from __future__ import annotations

from typing import Sequence, TYPE_CHECKING

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.quality import extract_usernames
from lib.quality.decisions import (
    PostImportSearchAction,
    post_import_search_action_if_known,
)
from lib.quality.dispatch_actions import DispatchAction

from lib.dispatch.quality_gate import QualityGatePlan
from lib.dispatch.types import QualityGateFn
from lib.terminal_outcomes import PendingImportTerminalOutcome, TerminalDenylist

if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB


def _apply_or_stage_transition(
    db: "PipelineDB",
    request_id: int,
    pending: PendingImportTerminalOutcome | None,
    transition: transitions.RequestTransition,
) -> PendingImportTerminalOutcome | None:
    if pending is not None:
        return pending.append_transitions(transition)
    transitions.require_transition_applied(
        finalize_request(db, request_id, transition)
    )
    return None


def _apply_or_stage_denylists(
    db: "PipelineDB",
    request_id: int,
    pending: PendingImportTerminalOutcome | None,
    usernames: set[str],
    reason: str,
    cooled_down_users: set[str] | None,
) -> PendingImportTerminalOutcome | None:
    if pending is not None:
        return pending.append_denylists(*(
            TerminalDenylist(username, reason, apply_cooldown=True)
            for username in sorted(usernames)
        ))
    for username in usernames:
        db.add_denylist(request_id, username, reason)
        if cooled_down_users is not None and db.check_and_apply_cooldown(username):
            cooled_down_users.add(username)
    return None


def _run_or_stage_quality_gate(
    quality_gate_fn: QualityGateFn,
    pending: PendingImportTerminalOutcome | None,
    **kwargs: object,
) -> PendingImportTerminalOutcome | None:
    if pending is None:
        quality_gate_fn(**kwargs)
        return None
    plan = quality_gate_fn(
        **kwargs,
        apply=False,
    )
    if not isinstance(plan, QualityGatePlan):
        return pending
    return pending.append_transitions(plan.transition).append_denylists(
        *plan.denylists
    )


def _resolve_post_import_search_policy(
    *,
    decision: str,
    action: DispatchAction,
    files: Sequence[object] | None,
    fallback_username: str | None,
) -> tuple[PostImportSearchAction | None, bool, set[str], list[object]]:
    """Resolve post-import search policy and its peer attribution once.

    Decision 19: force/manual imports resolve through the same canonical
    mapping as automatic imports — a force-imported provisional lossless
    copy gets the identical wanted + lossless-only requeue, never a
    silently terminal parking spot.
    """

    search_action = post_import_search_action_if_known(decision)
    should_denylist = (
        search_action.denylist
        if search_action is not None
        else action.denylist
    )
    file_list = list(files or ())
    usernames = extract_usernames(file_list) if should_denylist else set()
    if should_denylist and fallback_username:
        usernames.add(fallback_username)
    return search_action, should_denylist, usernames, file_list


def _apply_post_import_search_action(
    db: "PipelineDB",
    *,
    request_id: int,
    pending: PendingImportTerminalOutcome | None,
    decision: str,
    search_action: PostImportSearchAction | None,
    mark_done: bool,
    new_bitrate: int | None,
) -> PendingImportTerminalOutcome | None:
    """Apply the canonical retained-import requeue, when one is requested."""

    if search_action is None:
        return pending
    if search_action.status != "wanted":
        raise ValueError(
            "requeueing import decision mapped to non-wanted "
            f"status: {decision} -> {search_action.status}"
        )
    fields: dict[str, object] = {
        "search_filetype_override": search_action.search_filetype_override,
    }
    if mark_done and new_bitrate is not None:
        fields["min_bitrate"] = new_bitrate
    transition = transitions.RequestTransition.to_wanted_fields(
        from_status="imported",
        fields=fields,
    )
    return _apply_or_stage_transition(db, request_id, pending, transition)
