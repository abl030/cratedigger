"""State transition validation and side-effect declarations.

Pure functions for transition validation. The imperative apply_transition()
function delegates to pipeline_db methods and will be the single entry point
for all state mutations (Commit 4 migrates callers).

4 statuses: wanted, downloading, imported, manual
"""

from __future__ import annotations

from dataclasses import dataclass


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
