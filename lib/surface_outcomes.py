"""One home for the CLI ⇄ API surface-outcome convention (issue #1278).

The repository convention (`.claude/rules/code-quality.md` § "CLI ⇄ API
Surface Symmetry") pairs HTTP statuses with CLI exit codes: 2xx/0 success,
400/3 input validation, 404/2 not found, 409/4 wrong state, 422/3 semantic
violation, 503/5 transient. Services own exactly ONE outcome table — the
outcome → HTTP-status map — and derive their exit-code map here, so the two
operator surfaces cannot drift branch for branch.
``tests/test_surface_outcomes.py`` audits every registered map against this
module.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Final

KNOWN_HTTP_STATUSES: Final[frozenset[int]] = frozenset(
    {200, 202, 400, 404, 409, 422, 503}
)
"""Statuses a service outcome map may use; :func:`exit_codes_from_http`
refuses anything else. Some route-side outcome tables deliberately use
statuses outside this vocabulary (e.g. beets-distance's 410/500); their
CLI adapters pin those commands' historical exits through
``exit_overrides`` precisely because the convention maps no exit code
for them."""


def exit_code_for_http_status(
    status: int,
    exit_overrides: Mapping[int, int] | None = None,
) -> int:
    """Map one HTTP status onto the CLI's stable exit-code convention.

    Total by design: thin CLI HTTP adapters call this on whatever status the
    wire actually carried, so an unmapped status buckets to the transient
    exit 5 rather than raising. ``exit_overrides`` is how a routed command
    keeps the exact exit code it had while it executed in-process; it never
    invents a mapping — each entry pins one status to the code that
    command's own outcome table already used (issue #1063).
    """
    if exit_overrides is not None and status in exit_overrides:
        return exit_overrides[status]
    if 200 <= status < 300:
        return 0
    if status == 404:
        return 2
    if status in (400, 422):
        return 3
    if status == 409:
        return 4
    return 5


def exit_codes_from_http(http_status: Mapping[str, int]) -> dict[str, int]:
    """Derive a service's outcome → exit-code map from its HTTP map.

    Strict where :func:`exit_code_for_http_status` is total: services derive
    their exported exit map at module import time, so a status outside
    :data:`KNOWN_HTTP_STATUSES` raises loudly instead of silently bucketing
    a typo'd status into exit 5.
    """
    for outcome, status in http_status.items():
        if status not in KNOWN_HTTP_STATUSES:
            raise ValueError(
                f"outcome {outcome!r} maps to undocumented HTTP status {status}"
            )
    return {
        outcome: exit_code_for_http_status(status)
        for outcome, status in http_status.items()
    }
