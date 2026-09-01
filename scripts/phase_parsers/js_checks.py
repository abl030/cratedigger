"""Decodes `scripts/run_js_checks.sh` output, in both of its modes.

The marker is a three-field tab record, `CRATEDIGGER_JS_FAILURE<TAB>
identity<TAB>detail`, written by two producers: the wrapper itself, for
a `node --check` syntax failure and for the two things a suite cannot
report about its own death, and `tests/js_harness.mjs`, once per failed
assertion.

The two modes differ only in what makes the identity runnable, which is
why they are two functions rather than one taking a flag. `syntax`
identities are a bare `web/js/*.js` path; `unit` identities are
`<file>::<section>::<message>` from the harness, so the file is
everything before the first `::` and the rerun stays the whole suite
while the index entry stays per assertion.
"""

from __future__ import annotations

import shlex

from scripts.phase_parsers import (
    CheckFailure,
    PhaseFailures,
    PhaseLog,
    indexed_failure,
)

MARKER_PREFIX = "CRATEDIGGER_JS_FAILURE\t"


def _records(log: PhaseLog) -> list[tuple[str, str]]:
    """Every marker line as `(identity, detail)`.

    A malformed marker raises rather than being skipped: the wrapper and
    the harness both write this record with `printf`, so a two-field line
    means one of them is broken, and dropping it would turn a broken
    producer into a green phase.
    """
    records: list[tuple[str, str]] = []
    for line in log.text.splitlines():
        if not line.startswith(MARKER_PREFIX):
            continue
        fields = line.split("\t", 2)
        if len(fields) != 3:
            raise ValueError(f"malformed JavaScript failure marker: {line}")
        records.append((fields[1], fields[2]))
    return records


def parse_syntax_failures(log: PhaseLog) -> PhaseFailures:
    """`run_js_checks.sh syntax` — one identity per unparseable file."""
    return PhaseFailures(
        failures=tuple(
            indexed_failure(
                identity=identity,
                owner=identity,
                detail=detail,
                rerun_command=(
                    "node --check --input-type=module "
                    f"< {shlex.quote(identity)}"
                ),
                log=log.log_name,
            )
            for identity, detail in _records(log)
        )
    )


def parse_unit_failures(log: PhaseLog) -> PhaseFailures:
    """`run_js_checks.sh unit` — one identity per failed assertion."""
    failures: list[CheckFailure] = []
    for identity, detail in _records(log):
        owner = identity.split("::", 1)[0]
        failures.append(
            indexed_failure(
                identity=identity,
                owner=owner,
                detail=detail,
                rerun_command=f"node {shlex.quote(owner)}",
                log=log.log_name,
            )
        )
    return PhaseFailures(failures=tuple(failures))
