"""Operator mark that a request's installed copy is incomplete (issue #1241).

The mark (``album_requests.marked_incomplete_at``) is the operator's
decision, never a measured verdict — the daily library-completeness census
only informs it. Once set, the quality decider disregards the installed
side for any candidate beets proves whole
(``lib/quality/pipeline.py``, ``installed_marked_incomplete``), and a
terminal import acceptance whose candidate was proven whole clears it
(``lib/dispatch/outcome_actions.py``).

Both operator surfaces — ``pipeline-cli mark-incomplete`` and
``POST /api/pipeline/mark-incomplete`` — wrap :func:`set_incomplete_mark`
and share the outcome → exit-code / status-code maps below (CLI ⇄ API
surface symmetry).
"""

from dataclasses import dataclass
from typing import Literal, Protocol

from lib.surface_outcomes import exit_codes_from_http

IncompleteMarkOutcome = Literal[
    "marked",
    "cleared",
    "already_marked",
    "already_clear",
    "not_found",
    "replaced",
]

#: Outcome → HTTP status. Idempotent no-ops are successes.
INCOMPLETE_MARK_HTTP_STATUS: dict[str, int] = {
    "marked": 200,
    "cleared": 200,
    "already_marked": 200,
    "already_clear": 200,
    "not_found": 404,
    "replaced": 409,
}

#: Outcome → ``pipeline-cli`` exit code, derived branch for branch from the
#: HTTP map through the repository convention (``lib/surface_outcomes.py``).
INCOMPLETE_MARK_EXIT_CODES: dict[str, int] = exit_codes_from_http(
    INCOMPLETE_MARK_HTTP_STATUS
)


@dataclass(frozen=True)
class IncompleteMarkResult:
    """One outcome string per branch of the atomic mark mutation."""

    outcome: IncompleteMarkOutcome
    request_id: int


class _IncompleteMarkDB(Protocol):
    def set_marked_incomplete(
        self, request_id: int, *, marked: bool
    ) -> str: ...


_CANONICAL_OUTCOMES: tuple[IncompleteMarkOutcome, ...] = (
    "marked",
    "cleared",
    "already_marked",
    "already_clear",
    "not_found",
    "replaced",
)


def set_incomplete_mark(
    db: _IncompleteMarkDB,
    request_id: int,
    *,
    marked: bool,
) -> IncompleteMarkResult:
    """Set or clear the operator's incomplete mark on one request."""
    outcome = db.set_marked_incomplete(request_id, marked=marked)
    for candidate in _CANONICAL_OUTCOMES:
        if outcome == candidate:
            return IncompleteMarkResult(
                outcome=candidate, request_id=int(request_id)
            )
    raise ValueError(
        f"set_marked_incomplete returned unknown outcome {outcome!r}"
    )
