"""State transition validation and side-effect declarations.

Pure functions for transition validation. The imperative apply_transition()
delegates to pipeline_db methods and is the single entry point for all
state mutations.

4 statuses: wanted, downloading, imported, manual
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from lib.pipeline_db import PipelineDB

logger = logging.getLogger("soularr")


@dataclass(frozen=True)
class TransitionSideEffects:
    """What side effects a state transition requires.

    These flags tell the imperative layer (apply_transition) what
    db operations to perform alongside the status change.
    """
    clear_download_state: bool = False
    clear_retry_counters: bool = False
    record_attempt: bool = False


# Table of valid transitions and their required side effects.
# Any (from, to) pair not in this table is an invalid transition.
VALID_TRANSITIONS: dict[tuple[str, str], TransitionSideEffects] = {
    # Normal flow
    ("wanted", "downloading"): TransitionSideEffects(),
    ("downloading", "imported"): TransitionSideEffects(clear_download_state=True),
    ("downloading", "wanted"): TransitionSideEffects(
        clear_download_state=True, record_attempt=True),
    ("downloading", "manual"): TransitionSideEffects(clear_download_state=True),

    # Manual status changes
    ("wanted", "manual"): TransitionSideEffects(),
    # Idempotent reset (re-queue from wanted, field-only update)
    ("wanted", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # Re-queue (upgrade, retry from manual)
    ("imported", "wanted"): TransitionSideEffects(clear_retry_counters=True),
    ("manual", "wanted"): TransitionSideEffects(clear_retry_counters=True),

    # In-place update (quality gate accept, bitrate update)
    ("imported", "imported"): TransitionSideEffects(clear_download_state=True),

    # Admin overrides (force-import, web accept)
    ("manual", "imported"): TransitionSideEffects(clear_download_state=True),
    ("wanted", "imported"): TransitionSideEffects(clear_download_state=True),
}


def validate_transition(from_status: str, to_status: str) -> bool:
    """Check whether a status transition is valid."""
    return (from_status, to_status) in VALID_TRANSITIONS


def transition_side_effects(from_status: str, to_status: str) -> TransitionSideEffects:
    """Return the side-effect flags for a valid transition.

    Raises ValueError for invalid transitions.
    """
    fx = VALID_TRANSITIONS.get((from_status, to_status))
    if fx is None:
        raise ValueError(
            f"Invalid transition: {from_status!r} -> {to_status!r}")
    return fx


def apply_transition(
    db: "PipelineDB",
    request_id: int,
    to_status: str,
    **extra: Any,
) -> None:
    """Execute a validated state transition.

    This is the single entry point for all album_requests status mutations.
    It validates the transition, then delegates to the appropriate PipelineDB
    method with the correct side effects.

    Special keys extracted from extra:
        from_status: Current status (fetched from DB if not provided)
        search_filetype_override: For reset_to_wanted paths
        min_bitrate: For reset_to_wanted paths
        state_json: For set_downloading (wanted → downloading)
        attempt_type: For record_attempt (e.g. "download", "search")
        Everything else: passed to update_status as extra fields
    """
    # Extract special keys that control routing
    from_status = extra.pop("from_status", None)
    if from_status is not None:
        from_status = str(from_status)
    # Presence-based: only fields explicitly passed get written.
    # Omitted fields are preserved by reset_to_wanted / update_status.
    transition_fields: dict[str, object] = {}
    for _key in ("search_filetype_override", "min_bitrate", "prev_min_bitrate"):
        if _key in extra:
            transition_fields[_key] = extra.pop(_key)
    state_json = extra.pop("state_json", None)
    attempt_type = extra.pop("attempt_type", None)
    if from_status is None:
        row = db.get_request(request_id)
        if row is None:
            logger.warning(f"apply_transition: request {request_id} not found")
            return
        current = row["status"]
        assert isinstance(current, str)
        from_status = current

    if not validate_transition(from_status, to_status):
        logger.warning(
            f"apply_transition: invalid {from_status!r} -> {to_status!r} "
            f"for request {request_id}, proceeding anyway")

    fx = VALID_TRANSITIONS.get((from_status, to_status), TransitionSideEffects())

    # wanted → downloading: use set_downloading with JSONB state
    if to_status == "downloading" and state_json is not None:
        if not db.set_downloading(request_id, state_json):
            logger.warning(
                f"apply_transition: status guard prevented {from_status!r} -> "
                f"'downloading' for request {request_id} (album no longer wanted)")
        return

    # → wanted with counter reset: use reset_to_wanted
    if to_status == "wanted" and fx.clear_retry_counters:
        db.reset_to_wanted(request_id, **transition_fields)
        if fx.record_attempt and attempt_type:
            db.record_attempt(request_id, attempt_type)
        return

    # downloading → wanted: reset + record attempt
    if from_status == "downloading" and to_status == "wanted":
        db.reset_to_wanted(request_id, **transition_fields)
        if attempt_type:
            db.record_attempt(request_id, attempt_type)
        return

    # All other transitions: use update_status
    all_extra: dict[str, object] = dict(extra)
    all_extra.update(transition_fields)
    db.update_status(request_id, to_status, **all_extra)
