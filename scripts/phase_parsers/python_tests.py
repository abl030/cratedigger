"""The wire between `scripts/run_python_tests.py` and the failure index.

The scheduler already knows what failed and what it ran, so unlike every
other phase it does not print for a regex to recover: it encodes two
typed records onto its own stdout, and this module owns both ends of
that contract. The prefixes and Structs live here beside the decoder, so
the writer imports the shape it emits from the module that reads it.

`CheckFailureMarker` carries the exact unittest IDs, which is what lets
a rerun narrow from the whole scheduler to the tests that actually
failed. `CheckMetricsMarker` carries the counts the bundle records on
every phase, and the scheduler is the only phase that reports any.

Decoding is strict on purpose (`.claude/rules/code-quality.md` §
"Wire-boundary types"): a marker that does not fit its Struct raises,
and the coordinator turns that into an infrastructure failure rather
than dropping the record and reporting a green phase.
"""

from __future__ import annotations

import shlex

import msgspec

from scripts.phase_parsers import (
    CheckFailure,
    PhaseFailures,
    PhaseLog,
    indexed_failure,
)

FAILURE_MARKER_PREFIX = "CRATEDIGGER_CHECK_FAILURE "
METRICS_MARKER_PREFIX = "CRATEDIGGER_CHECK_METRICS "


class CheckFailureMarker(msgspec.Struct, frozen=True):
    """Structured failure marker emitted by the Python test scheduler."""

    identity: str
    owner: str
    detail: str
    test_ids: tuple[str, ...] = ()


class CheckMetricsMarker(msgspec.Struct, frozen=True):
    """Structured completion metrics emitted by a validation phase."""

    tests_run: int = 0
    targets_run: int = 0
    scheduled_targets: int = 0


def parse_failures(log: PhaseLog) -> PhaseFailures:
    """Decode both marker kinds; a later metrics marker replaces an earlier."""
    failures: list[CheckFailure] = []
    metrics = CheckMetricsMarker()
    for line in log.text.splitlines():
        if line.startswith(FAILURE_MARKER_PREFIX):
            marker = msgspec.json.decode(
                line.removeprefix(FAILURE_MARKER_PREFIX).encode(),
                type=CheckFailureMarker,
            )
            failures.append(
                indexed_failure(
                    identity=marker.identity,
                    owner=marker.owner,
                    detail=marker.detail,
                    rerun_command=(
                        "python3 -m unittest " + shlex.join(marker.test_ids)
                        if marker.test_ids
                        else log.rerun_command
                    ),
                    log=log.log_name,
                    test_ids=marker.test_ids,
                )
            )
        elif line.startswith(METRICS_MARKER_PREFIX):
            metrics = msgspec.json.decode(
                line.removeprefix(METRICS_MARKER_PREFIX).encode(),
                type=CheckMetricsMarker,
            )
    return PhaseFailures(
        failures=tuple(failures),
        tests_run=metrics.tests_run,
        targets_run=metrics.targets_run,
        scheduled_targets=metrics.scheduled_targets,
    )
