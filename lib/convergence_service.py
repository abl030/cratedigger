"""Operator-visible search convergence for provisional lossless holdings.

Convergence is an observation, never proof and never an automatic policy
change.  The only mutation is the operator's explicit, reversible decision to
stop searching.  Authority: issue #829's correction and split decision:
https://github.com/abl030/cratedigger/issues/829#issuecomment-5088076688
https://github.com/abl030/cratedigger/issues/829#issuecomment-5148394123
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from datetime import datetime
from typing import Literal, Protocol

import msgspec
import psycopg2

CLIFF_BIN_HZ = 500
MIN_DISTINCT_PEERS = 5
MIN_QUALIFYING_OBSERVATIONS = 5
SIGNAL_TOKEN_PATTERN = r"^[0-9a-f]{64}$"
_SIGNAL_TOKEN_RE = re.compile(SIGNAL_TOKEN_PATTERN)


class ConvergenceObservation(msgspec.Struct, frozen=True):
    """One chronological candidate observation used by the reference model."""

    log_id: int
    contributor_usernames: tuple[str, ...]
    snapshot_fingerprint: str
    codec: str
    cliff_hz: int | None
    observed_at: datetime
    eligible: bool
    direct_attribution: bool = True


class ConvergenceSignal(msgspec.Struct, frozen=True):
    """Current raw-cliff constancy signal for one exact request."""

    request_id: int
    observation_count: int
    distinct_peer_count: int
    distinct_candidate_snapshot_count: int
    distinct_codec_count: int
    cliff_hz: int
    raw_cliff_min_hz: int
    raw_cliff_max_hz: int
    cliff_spread_hz: int
    latest_qualifying_log_id: int
    first_observed_at: datetime
    latest_observed_at: datetime
    signal_token: str


StopConvergenceOutcome = Literal[
    "stopped", "not_found", "wrong_state", "stale", "not_converged",
    "unavailable",
]


class StopConvergedSearchResult(msgspec.Struct, frozen=True):
    outcome: StopConvergenceOutcome
    request_id: int
    signal: ConvergenceSignal | None = None
    observed_status: str | None = None


def _cliff_bin(cliff_hz: int) -> int:
    """Nearest 500 Hz, matching PostgreSQL ``ROUND`` for non-negative Hz."""
    return ((cliff_hz + CLIFF_BIN_HZ // 2) // CLIFF_BIN_HZ) * CLIFF_BIN_HZ


def normalize_contributor_usernames(
    contributor_usernames: Iterable[str],
) -> tuple[str, ...]:
    """Normalize structured Soulseek identities without parsing display text."""
    return tuple(sorted({
        username
        for raw in contributor_usernames
        if (username := raw.strip().lower())
    }))


def parse_signal_token(value: str) -> str:
    """Validate the opaque convergence identity at an adapter boundary."""
    if _SIGNAL_TOKEN_RE.fullmatch(value) is None:
        raise ValueError("signal token must be exactly 64 lowercase hex characters")
    return value


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
        (
            row for row in observations
            if (
                row.eligible
                and row.direct_attribution
                and normalize_contributor_usernames(row.contributor_usernames)
            )
        ),
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
    peers: set[str] = {
        username
        for row in run
        for username in normalize_contributor_usernames(
            row.contributor_usernames,
        )
    }
    if (
        len(run) < MIN_QUALIFYING_OBSERVATIONS
        or len(peers) < MIN_DISTINCT_PEERS
    ):
        return None
    raw_cliffs = [
        row.cliff_hz for row in run if row.cliff_hz is not None
    ]
    raw_cliff_min_hz = min(raw_cliffs)
    raw_cliff_max_hz = max(raw_cliffs)
    return ConvergenceSignal(
        request_id=int(request_id),
        observation_count=len(run),
        distinct_peer_count=len(peers),
        distinct_candidate_snapshot_count=len({
            row.snapshot_fingerprint for row in run
        }),
        distinct_codec_count=len({
            row.codec.strip().lower() or "unknown" for row in run
        }),
        cliff_hz=current_bin,
        raw_cliff_min_hz=raw_cliff_min_hz,
        raw_cliff_max_hz=raw_cliff_max_hz,
        cliff_spread_hz=raw_cliff_max_hz - raw_cliff_min_hz,
        latest_qualifying_log_id=run[0].log_id,
        first_observed_at=run[-1].observed_at,
        latest_observed_at=run[0].observed_at,
        signal_token="reference-only",
    )


class SupportsConvergenceStop(Protocol):
    def stop_search_for_convergence(
        self,
        request_id: int,
        *,
        signal_token: str,
    ) -> StopConvergedSearchResult: ...


class ConvergenceStopService:
    """Canonical action seam shared by HTTP and CLI adapters."""

    def __init__(self, db: SupportsConvergenceStop) -> None:
        self._db = db

    def stop(
        self,
        request_id: int,
        *,
        signal_token: str,
    ) -> StopConvergedSearchResult:
        try:
            return self._db.stop_search_for_convergence(
                int(request_id),
                signal_token=str(signal_token),
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            return StopConvergedSearchResult(
                outcome="unavailable", request_id=int(request_id),
            )
