"""What a validation phase's log says, in the one shape the suite indexes.

Issue #1313. `scripts/run_test_suite.py` used to decode every tool's
output itself, in one 144-line function branching on a stringly
`PhaseSpec.parser` tag: a `CRATEDIGGER_JS_FAILURE` tab record, pyright's
`file:line:col - error:`, ruff's two output formats paired across lines,
vulture's finding lines plus its whitelist-freshness diff, and the
scheduler's own typed markers. Adding a phase meant editing the
coordinator; reading the coordinator meant reading five dialects it has
no other reason to know.

A `PhaseSpec` now names the callable that reads its log, and each
callable lives in the module named after the wrapper whose output it
decodes (`scripts/run_ruff.sh` → `phase_parsers/ruff.py`, and so on).
The coordinator runs a command, hands the log text to whatever that
phase named, and indexes what comes back. It knows zero dialects.

This module is the contract between the two and holds no dialect of its
own: the failure record the bundle persists, the log a parser is handed,
what it may return, and what it may raise.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import msgspec


class CheckFailure(msgspec.Struct, frozen=True):
    """One compact failure-index entry linking to complete evidence."""

    identity: str
    owner: str
    detail: str
    rerun_command: str
    log: str
    test_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class PhaseLog:
    """One finished phase's complete output, plus what indexing it needs.

    `rerun_command` is the phase's own runnable command. A parser may use
    it verbatim (pyright and vulture do — their findings are not
    separately runnable) or derive something narrower from the record it
    just decoded (ruff reruns one file; the Python scheduler reruns the
    exact failing test IDs).
    """

    text: str
    log_name: str
    rerun_command: str


@dataclass(frozen=True)
class PhaseFailures:
    """What a parser found: the index entries, and any counts it read.

    The counts are zero for every phase whose tool reports none, which is
    all of them but the Python scheduler. They live here rather than in
    that dialect's own return type because the bundle records them on
    every phase.
    """

    failures: tuple[CheckFailure, ...] = ()
    tests_run: int = 0
    targets_run: int = 0
    scheduled_targets: int = 0


#: Reads one phase's log into its failure index. `PhaseSpec.parser` is one
#: of these, so the set of dialects is open and the coordinator's
#: knowledge of them is nil.
PhaseFailureParser = Callable[[PhaseLog], PhaseFailures]

#: What a parser is allowed to raise when a log is malformed. The
#: coordinator catches exactly these and reports the phase as an
#: infrastructure failure rather than a clean pass, so a dialect that
#: needs a new one adds it HERE — the alternative is the coordinator
#: growing a bare `except Exception`, which would swallow a real defect
#: in a parser as if the tool had misbehaved.
PARSER_ERRORS: tuple[type[Exception], ...] = (
    OSError,
    ValueError,
    msgspec.DecodeError,
    msgspec.ValidationError,
)


def indexed_failure(
    *,
    identity: str,
    owner: str,
    detail: str,
    rerun_command: str,
    log: str,
    test_ids: tuple[str, ...] = (),
) -> CheckFailure:
    """Build one index entry. Keyword-only so no field is passed by luck."""
    return CheckFailure(
        identity=identity,
        owner=owner,
        detail=detail,
        rerun_command=rerun_command,
        log=log,
        test_ids=test_ids,
    )
