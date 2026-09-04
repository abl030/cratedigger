"""Post-import search-policy application.

One owner for what follows a successful retained import: the canonical
decision->(status, override, denylist) resolution, peer attribution, and gate
staging. Quality/search policy is mode-blind; terminal persistence arbitrates
operator-owned search state against the request row current at commit time.

``finalize_request`` is the module-local DI seam for the transition-only
writer below, same shape as its sibling ``harness.import_one``. The
denylist-batch writer commits through
``PipelineDB.persist_request_policy_outcome`` instead (issue #1355 item
A2), which needs no such seam.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from lib import transitions

# Module-level DI seam for ``transitions.finalize_request``.
finalize_request = transitions.finalize_request

from lib.dispatch.quality_gate import QualityGatePlan
from lib.dispatch.types import QualityGateFn
from lib.quality import extract_usernames, resolve_retained_search_override
from lib.quality.decisions import (
    PostImportSearchAction,
    post_import_search_action_if_known,
)
from lib.quality.dispatch_actions import decision_denylists
from lib.terminal_outcomes import (
    PendingImportTerminalOutcome,
    RequestPolicyOutcome,
    TerminalDenylist,
)

if TYPE_CHECKING:
    from lib.dispatch.types import DispatchDB


def _apply_or_stage_transition(
    db: DispatchDB,
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
    db: DispatchDB,
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
    # Every denylist entry (and its cooldown check, when the caller wants
    # one tracked) commits together in one PostgreSQL transaction (issue
    # #1355 item A2) — this used to be a separate ``add_denylist`` plus
    # ``check_and_apply_cooldown`` autocommit per username, so a crash
    # mid-loop could leave some peers denylisted and others not.
    # ``cooled_down_users is None`` means the caller never wants a cooldown
    # write for this batch, exactly as the prior loop skipped the check
    # entirely in that case.
    result = db.persist_request_policy_outcome(RequestPolicyOutcome(
        request_id=request_id,
        denylists=tuple(
            TerminalDenylist(
                username, reason,
                apply_cooldown=cooled_down_users is not None,
            )
            for username in sorted(usernames)
        ),
    ))
    if cooled_down_users is not None:
        cooled_down_users.update(result.cooled_down_users)
    return None


def _run_or_stage_quality_gate(
    quality_gate_fn: QualityGateFn,
    pending: PendingImportTerminalOutcome | None,
    *,
    db: DispatchDB,
    request_id: int,
    **kwargs: object,
) -> PendingImportTerminalOutcome | None:
    if pending is None:
        quality_gate_fn(db=db, request_id=request_id, **kwargs)
        return None
    plan = quality_gate_fn(
        db=db,
        request_id=request_id,
        **kwargs,
        apply=False,
    )
    if not isinstance(plan, QualityGatePlan):
        return pending
    pending = pending.append_transitions(plan.transition).append_denylists(
        *plan.denylists
    )
    if plan.successful_terminal_acceptance:
        pending = pending.mark_successful_terminal_acceptance()
    return pending


def _resolve_post_import_search_policy(
    *,
    decision: str,
    files: Sequence[object] | None,
    fallback_username: str | None,
) -> tuple[PostImportSearchAction | None, bool, set[str], list[object]]:
    """Resolve post-import search policy and its peer attribution once.

    Decision 19: force imports resolve through the same canonical quality and
    search mapping as automatic imports. Terminal persistence applies that
    mapping without overwriting current operator-owned search state.
    """

    search_action = post_import_search_action_if_known(decision)
    should_denylist = decision_denylists(decision)
    file_list = list(files or ())
    usernames = extract_usernames(file_list) if should_denylist else set[str]()
    if should_denylist and fallback_username:
        usernames.add(fallback_username)
    return search_action, should_denylist, usernames, file_list


def _apply_post_import_search_action(
    db: DispatchDB,
    *,
    request_id: int,
    pending: PendingImportTerminalOutcome | None,
    decision: str,
    search_action: PostImportSearchAction | None,
    mark_done: bool,
    new_bitrate: int | None,
) -> PendingImportTerminalOutcome | None:
    """Apply or stage the canonical retained-import search policy."""

    if search_action is None:
        return pending
    if search_action.status != "wanted":
        raise ValueError(
            "requeueing import decision mapped to non-wanted "
            f"status: {decision} -> {search_action.status}"
        )
    request = db.get_request(request_id)
    raw_existing_override = (
        request.get("search_filetype_override")
        if request is not None
        else None
    )
    existing_override = (
        raw_existing_override
        if isinstance(raw_existing_override, str)
        else None
    )
    fields: dict[str, object] = {
        "search_filetype_override": resolve_retained_search_override(
            existing_override,
            search_action.search_filetype_override,
        ),
    }
    if mark_done and new_bitrate is not None:
        fields["min_bitrate"] = new_bitrate
    transition = transitions.RequestTransition.to_wanted_fields(
        from_status="imported",
        fields=fields,
    )
    return _apply_or_stage_transition(db, request_id, pending, transition)
