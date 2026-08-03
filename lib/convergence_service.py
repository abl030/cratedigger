"""Operator-visible search convergence for provisional lossless holdings.

Convergence is an observation, never proof and never an automatic policy
change.  The only mutation is the operator's explicit, reversible decision to
stop searching.  Authority: issue #829's correction and split decision:
https://github.com/abl030/cratedigger/issues/829#issuecomment-5088076688
https://github.com/abl030/cratedigger/issues/829#issuecomment-5148394123
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Literal, Protocol

import msgspec

CLIFF_BIN_HZ = 500
MIN_DISTINCT_PEERS = 5


class ConvergenceObservation(msgspec.Struct, frozen=True):
    """One chronological candidate observation used by the reference model."""

    log_id: int
    peer: str
    snapshot_fingerprint: str
    cliff_hz: int | None
    observed_at: datetime
    eligible: bool


class ConvergenceSignal(msgspec.Struct, frozen=True):
    """Current raw-cliff constancy signal for one exact request."""

    request_id: int
    observation_count: int
    distinct_peer_count: int
    distinct_candidate_snapshot_count: int
    cliff_hz: int
    latest_qualifying_log_id: int
    first_observed_at: datetime
    latest_observed_at: datetime


StopConvergenceOutcome = Literal[
    "stopped", "not_found", "wrong_state", "stale", "not_converged",
]


class StopConvergedSearchResult(msgspec.Struct, frozen=True):
    outcome: StopConvergenceOutcome
    request_id: int
    signal: ConvergenceSignal | None = None
    observed_status: str | None = None


def _cliff_bin(cliff_hz: int) -> int:
    """Nearest 500 Hz, matching PostgreSQL ``ROUND`` for non-negative Hz."""
    return ((cliff_hz + CLIFF_BIN_HZ // 2) // CLIFF_BIN_HZ) * CLIFF_BIN_HZ


def derive_convergence_signal(
    request_id: int,
    observations: Iterable[ConvergenceObservation],
) -> ConvergenceSignal | None:
    """Reference derivation over newest consecutive eligible observations.

    Ineligible legacy, world-error, non-exact, high-distance, or non-source
    rows are ignored.  Among eligible observations, a missing cliff is an
    upward break and a different 500 Hz bin resets the current run.
    """
    eligible = sorted(
        (row for row in observations if row.eligible),
        key=lambda row: (row.observed_at, row.log_id),
        reverse=True,
    )
    if not eligible or eligible[0].cliff_hz is None:
        return None
    current_bin = _cliff_bin(eligible[0].cliff_hz)
    run: list[ConvergenceObservation] = []
    for row in eligible:
        if row.cliff_hz is None or _cliff_bin(row.cliff_hz) != current_bin:
            break
        run.append(row)
    peers = {row.peer for row in run}
    if len(peers) < MIN_DISTINCT_PEERS:
        return None
    return ConvergenceSignal(
        request_id=int(request_id),
        observation_count=len(run),
        distinct_peer_count=len(peers),
        distinct_candidate_snapshot_count=len({
            row.snapshot_fingerprint for row in run
        }),
        cliff_hz=current_bin,
        latest_qualifying_log_id=run[0].log_id,
        first_observed_at=run[-1].observed_at,
        latest_observed_at=run[0].observed_at,
    )


class SupportsConvergenceStop(Protocol):
    def stop_search_for_convergence(
        self,
        request_id: int,
        *,
        latest_qualifying_log_id: int,
        cliff_hz: int,
    ) -> StopConvergedSearchResult: ...


class ConvergenceStopService:
    """Canonical action seam shared by HTTP and CLI adapters."""

    def __init__(self, db: SupportsConvergenceStop) -> None:
        self._db = db

    def stop(
        self,
        request_id: int,
        *,
        latest_qualifying_log_id: int,
        cliff_hz: int,
    ) -> StopConvergedSearchResult:
        return self._db.stop_search_for_convergence(
            int(request_id),
            latest_qualifying_log_id=int(latest_qualifying_log_id),
            cliff_hz=int(cliff_hz),
        )
