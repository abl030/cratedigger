"""Decodes `scripts/run_pyright_checks.py` output.

That wrapper runs both typing contracts concurrently and prints each
one's diagnostics through, so what reaches the log is pyright's own
default text format: `<file>:<line>:<col> - error: <message>`. Warnings
and informations are not indexed; only `- error:` lines are, because
only they fail the phase.

A diagnostic is not separately runnable, so the rerun stays the phase's
own command.
"""

from __future__ import annotations

import re

from scripts.phase_parsers import (
    CheckFailure,
    PhaseFailures,
    PhaseLog,
    indexed_failure,
)

_DIAGNOSTIC = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+) - error: (?P<detail>.+)$"
)


def parse_failures(log: PhaseLog) -> PhaseFailures:
    """One index entry per pyright error, identified by its position."""
    failures: list[CheckFailure] = []
    for line in log.text.splitlines():
        match = _DIAGNOSTIC.match(line)
        if match is None:
            continue
        owner = match.group("owner").strip()
        failures.append(
            indexed_failure(
                identity=(
                    f"{owner}:{match.group('line')}:{match.group('column')}"
                ),
                owner=owner,
                detail=match.group("detail"),
                rerun_command=log.rerun_command,
                log=log.log_name,
            )
        )
    return PhaseFailures(failures=tuple(failures))
