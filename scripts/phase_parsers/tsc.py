"""Decodes `scripts/run_tsc.sh` output.

That wrapper runs tsc with `--pretty false`, so the log carries tsc's
plain diagnostic format, which places the position in parentheses rather
than after colons:

    web/js/browse.js(605,26): error TS2339: Property 'dataset' does not
    exist on type 'Element'.

An assignability failure adds indented continuation lines under that
first one. They carry no position of their own and are left to the
complete phase log, exactly as pyright's sub-diagnostics are: the
indexed detail is the top line, which names the file, the rule and the
finding.

tsc also reports failures with no file at all — a missing or unparsable
project file, an unknown option — as a bare `error TSxxxx:` line, and
exits 1 rather than the 2 it uses for diagnostics in the source. Those
are indexed too, keyed on the rule number, because a phase that failed
with an empty index is the one shape the failure bundle cannot explain.
Their owner is the checker rather than a file, since the path (when
there is one) is inside the message and reading it back out would be
guessing.

A diagnostic is not separately runnable — tsc checks a project, not a
file — so the rerun stays the phase's own command.
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
    r"^(?P<owner>.+?)\((?P<line>\d+),(?P<column>\d+)\): "
    r"error (?P<code>TS\d+): (?P<detail>.+)$"
)
_FILELESS = re.compile(r"^error (?P<code>TS\d+): (?P<detail>.+)$")

#: What a fileless diagnostic is attributed to. tsc is the only thing that
#: knows why it could not start; the wrapper is where a reader goes next.
_CHECKER_OWNER = "scripts/run_tsc.sh"


def parse_failures(log: PhaseLog) -> PhaseFailures:
    """One index entry per tsc error, identified by its position."""
    failures: list[CheckFailure] = []
    for line in log.text.splitlines():
        if match := _DIAGNOSTIC.match(line):
            owner = match.group("owner").strip()
            failures.append(
                indexed_failure(
                    identity=(
                        f"{owner}:{match.group('line')}:{match.group('column')}"
                    ),
                    owner=owner,
                    detail=f"{match.group('code')} {match.group('detail')}",
                    rerun_command=log.rerun_command,
                    log=log.log_name,
                )
            )
            continue
        if match := _FILELESS.match(line):
            failures.append(
                indexed_failure(
                    identity=f"{_CHECKER_OWNER}:{match.group('code')}",
                    owner=_CHECKER_OWNER,
                    detail=f"{match.group('code')} {match.group('detail')}",
                    rerun_command=log.rerun_command,
                    log=log.log_name,
                )
            )
    return PhaseFailures(failures=tuple(failures))
