"""Decodes `scripts/run_ruff.sh` output, in either output format.

The wrapper passes `--output-format` from `CRATEDIGGER_RUFF_OUTPUT_FORMAT`,
defaulting to `full`, so the log carries one of two shapes and this is
the only parser that has to hold state across lines:

* `concise`, one violation per line —
  `path:line:col: CODE message`
* `full`, the header and the location on separate lines —
  `CODE message`, then `  --> path:line:col`

The pairing is deliberately forgiving in one direction only. A header
with no location that follows contributes nothing, because `full` also
prints a `help:` block and a source excerpt between violations and a
header only becomes an index entry once its location arrives. A location
with no pending header is ignored for the same reason.

Ruff runs per file, so the rerun narrows to the one file that failed.
"""

from __future__ import annotations

import re
import shlex

from scripts.phase_parsers import (
    CheckFailure,
    PhaseFailures,
    PhaseLog,
    indexed_failure,
)

_CONCISE = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+): "
    r"(?P<code>[A-Z]+\d+) (?P<detail>.+)$"
)
_FULL_HEADER = re.compile(r"^(?P<code>[A-Z]+\d+) (?P<detail>.+)$")
_FULL_LOCATION = re.compile(
    r"^\s*-->\s*(?P<owner>.+?):(?P<line>\d+):(?P<column>\d+)$"
)


def _rerun_for(owner: str) -> str:
    return f"bash scripts/run_ruff.sh {shlex.quote(owner)}"


def parse_failures(log: PhaseLog) -> PhaseFailures:
    """One index entry per violation, from whichever format ruff wrote."""
    failures: list[CheckFailure] = []
    pending: tuple[str, str] | None = None
    for line in log.text.splitlines():
        if match := _CONCISE.match(line):
            owner = match.group("owner").strip()
            failures.append(
                indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=f"{match.group('code')} {match.group('detail')}",
                    rerun_command=_rerun_for(owner),
                    log=log.log_name,
                )
            )
            continue
        if match := _FULL_HEADER.match(line):
            pending = (match.group("code"), match.group("detail"))
            continue
        if pending is not None and (match := _FULL_LOCATION.match(line)):
            owner = match.group("owner").strip()
            code, detail = pending
            failures.append(
                indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=f"{code} {detail}",
                    rerun_command=_rerun_for(owner),
                    log=log.log_name,
                )
            )
            pending = None
    return PhaseFailures(failures=tuple(failures))
