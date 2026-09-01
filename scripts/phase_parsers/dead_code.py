"""Decodes `scripts/find_dead_code.sh` output.

That wrapper reports two different things and this parser reads both:

* vulture's own findings —
  `path:line: unused function 'name' (60% confidence)`
* the whitelist-freshness diff it prints when
  `tools/vulture/whitelist.py` no longer matches the candidate baseline,
  whose added lines read
  `+identifier  # unused function (path:line)`

The second is why the phase's failure exit code is 3 rather than 1: a
stale whitelist is a distinct verdict from live dead code, and both need
to reach the index by identifier so a reader knows which name moved.

Neither finding is separately runnable, so the rerun stays the phase's
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

_FINDING = re.compile(
    r"^(?P<owner>.+?):(?P<line>\d+): (?P<detail>.+? \(\d+% confidence\))$"
)
_FRESHNESS = re.compile(
    r"^\+(?P<identity>\S+)\s+# (?P<detail>unused .+?) "
    r"\((?P<owner>[^:]+):(?P<line>\d+)\)$"
)


def parse_failures(log: PhaseLog) -> PhaseFailures:
    """One index entry per finding, from either of the wrapper's reports."""
    failures: list[CheckFailure] = []
    for line in log.text.splitlines():
        if match := _FINDING.match(line):
            owner = match.group("owner").strip()
            failures.append(
                indexed_failure(
                    identity=f"{owner}:{match.group('line')}",
                    owner=owner,
                    detail=match.group("detail"),
                    rerun_command=log.rerun_command,
                    log=log.log_name,
                )
            )
            continue
        if match := _FRESHNESS.match(line):
            owner = match.group("owner").strip()
            failures.append(
                indexed_failure(
                    identity=f"{owner}:{match.group('line')}",
                    owner=owner,
                    detail=(
                        f"{match.group('identity')}: {match.group('detail')}"
                    ),
                    rerun_command=log.rerun_command,
                    log=log.log_name,
                )
            )
    return PhaseFailures(failures=tuple(failures))
