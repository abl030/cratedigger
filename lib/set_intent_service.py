"""Canonical lossless-on-disk intent service (issue #1278 item 2).

Both operator surfaces — ``pipeline-cli set-intent`` and
``POST /api/pipeline/set-intent`` — wrap :func:`set_lossless_intent` and
share the outcome table below (CLI ⇄ API surface symmetry; model:
``lib/incomplete_mark_service.py``). Lifecycle compare-and-set misses are
returned as the shared :class:`lib.transitions.TransitionConflict` and
rendered by each surface's existing conflict adapter.

``lossless`` keeps lossless on disk for this request (overriding the global
``verified_lossless_target``); ``default`` lets the pipeline decide. Setting
``lossless`` on an ``imported`` request re-queues it to search for a
lossless source.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from lib import transitions
from lib.pipeline_db.rows import AlbumRequestRow
from lib.quality import QUALITY_LOSSLESS, should_clear_lossless_search_override
from lib.surface_outcomes import exit_codes_from_http

SetIntentOutcome = Literal[
    "updated",
    "requeued",
    "not_found",
    "initializing",
    "downloading",
]

#: Outcome → HTTP status. ``initializing`` and ``downloading`` are
#: wrong-state refusals (the request is service-owned right now).
#: Lifecycle conflicts are not rows here — they carry their own
#: ``TransitionConflict`` and map through
#: ``transitions.transition_conflict_http_status``.
SET_INTENT_HTTP_STATUS: dict[str, int] = {
    "updated": 200,
    "requeued": 200,
    "not_found": 404,
    "initializing": 409,
    "downloading": 409,
}

#: Outcome → ``pipeline-cli`` exit code, derived branch for branch from the
#: HTTP map through the repository convention (``lib/surface_outcomes.py``).
SET_INTENT_EXIT_CODES: dict[str, int] = exit_codes_from_http(
    SET_INTENT_HTTP_STATUS
)


@dataclass(frozen=True)
class SetIntentResult:
    """One outcome string per non-conflict branch of the intent mutation."""

    outcome: SetIntentOutcome
    request_id: int
    intent: Literal["lossless", "default"]
    target_format: str | None
    old_target_format: str | None = None
    artist_name: str = ""
    album_title: str = ""


class SetIntentDB(transitions.TransitionsDB, Protocol):
    """The transition engine's surface plus the metadata CAS writer."""

    def update_request_fields(
        self,
        request_id: int,
        *,
        expected_status: str | None = None,
        **extra: object,
    ) -> bool: ...


FinalizeRequestFn = Callable[
    [transitions.TransitionsDB, int, transitions.RequestTransition],
    transitions.TransitionResult,
]


def set_lossless_intent(
    db: SetIntentDB,
    request_id: int,
    *,
    intent: Literal["lossless", "default"],
    finalize_request_fn: FinalizeRequestFn = transitions.finalize_operator_request,
) -> SetIntentResult | transitions.TransitionConflict:
    """Toggle the lossless-on-disk intent for one request."""
    target_format = QUALITY_LOSSLESS if intent == "lossless" else None

    def result(outcome: SetIntentOutcome, row: AlbumRequestRow) -> SetIntentResult:
        return SetIntentResult(
            outcome=outcome,
            request_id=request_id,
            intent=intent,
            target_format=target_format,
            old_target_format=row["target_format"],
            artist_name=str(row["artist_name"]),
            album_title=str(row["album_title"]),
        )

    def not_found() -> SetIntentResult:
        return SetIntentResult(
            outcome="not_found",
            request_id=request_id,
            intent=intent,
            target_format=target_format,
        )

    req = db.get_request(request_id)
    if req is None:
        return not_found()
    if req["status"] == "initializing":
        return result("initializing", req)
    if req["status"] == "downloading":
        return result("downloading", req)
    if req["status"] == "replaced":
        # ``replaced`` is frozen (no valid outgoing edge), so this reports
        # the canonical invalid-edge conflict through the transition engine
        # rather than a bespoke error. Should that edge ever become valid,
        # the revived row simply continues below like any other request.
        revive = finalize_request_fn(
            db,
            request_id,
            transitions.RequestTransition.to_wanted(from_status="replaced"),
        )
        if isinstance(revive, transitions.TransitionConflict):
            return revive
        refreshed = db.get_request(request_id)
        if refreshed is None:
            return not_found()
        req = refreshed

    old_target = req["target_format"]

    if req["status"] == "imported" and target_format:
        # Re-queue to search for a lossless source.
        requeue = finalize_request_fn(
            db,
            request_id,
            transitions.RequestTransition.to_wanted(
                from_status="imported",
                search_filetype_override=QUALITY_LOSSLESS,
                min_bitrate=req["min_bitrate"],
            ),
        )
        if isinstance(requeue, transitions.TransitionConflict):
            return requeue
        applied = db.update_request_fields(
            request_id,
            expected_status="wanted",
            target_format=target_format,
        )
        if not applied:
            return transitions.request_fields_cas_conflict(
                db, request_id, expected_status="wanted",
            )
        return result("requeued", req)

    update_fields: dict[str, object] = {"target_format": target_format}
    if should_clear_lossless_search_override(
        new_target_format=target_format,
        old_target_format=old_target,
        search_filetype_override=req["search_filetype_override"],
    ):
        update_fields["search_filetype_override"] = None
    applied = db.update_request_fields(
        request_id,
        expected_status=str(req["status"]),
        **update_fields,
    )
    if not applied:
        return transitions.request_fields_cas_conflict(
            db, request_id, expected_status=str(req["status"]),
        )
    return result("updated", req)
