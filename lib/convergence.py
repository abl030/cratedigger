"""Ordered, failure-isolated convergence wiring for one pipeline cycle."""
from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from importlib import import_module
from types import MappingProxyType
from typing import cast, Protocol

from lib.context import CratediggerContext

logger = logging.getLogger("cratedigger")


class ConvergenceGroup(Enum):
    """The two quiescent windows in which convergence work may run."""

    PHASE_ZERO = "phase_zero"
    END_OF_CYCLE = "end_of_cycle"


StepCallable = Callable[[CratediggerContext], object]


@dataclass(frozen=True)
class ConvergenceStep:
    """One best-effort cycle step and its operator-facing failure log."""

    name: str
    run: StepCallable
    failure_message: str
    module_name: str | None = None
    callable_name: str | None = None


class ExceptionLogger(Protocol):
    def exception(self, msg: str, *args: object) -> object: ...


def resolve_convergence_target(
    module_name: str,
    callable_name: str,
) -> StepCallable:
    """Import and validate one lazy production target without executing it."""
    module = import_module(module_name)
    target = getattr(module, callable_name)
    if not callable(target):
        raise TypeError(f"{module_name}.{callable_name} is not callable")
    return cast(StepCallable, target)


def _lazy_step(
    module_name: str,
    callable_name: str,
    failure_message: str,
) -> ConvergenceStep:
    """Register a step without importing its implementation up front.

    Imports used to live inside each step's ``try`` block in
    ``cratedigger.main``.  Resolution stays inside the runner's isolation
    boundary so a broken optional dependency cannot abort the whole cycle.
    """
    def run(ctx: CratediggerContext) -> object:
        return resolve_convergence_target(module_name, callable_name)(ctx)

    return ConvergenceStep(
        name=callable_name,
        run=run,
        failure_message=failure_message,
        module_name=module_name,
        callable_name=callable_name,
    )


_SLSKD_ORPHAN_FAILURE = (
    "SLSKD ORPHAN: convergence failed; continuing with the cycle.")
_DISK_REAP_FAILURE = "DISK-REAP: sweep failed; continuing with the cycle."
_SEARCH_LEDGER_FAILURE = (
    "SEARCH-LEDGER: sweep failed; continuing with the cycle.")
_TRANSFER_LEDGER_FAILURE = (
    "TRANSFER-LEDGER: prune failed; continuing with the cycle.")
_HARVEST_FAILURE = (
    "HARVEST: pre-purge evidence harvest failed; continuing with the cycle.")
_COMPLETED_PURGE_FAILURE = (
    "COMPLETED-PURGE: sweep failed; continuing with the cycle.")


# Ordering is policy data.  A new convergence step is one registration here
# and inherits the runner's cycle-preserving failure isolation automatically.
CONVERGENCE_STEPS: Mapping[ConvergenceGroup, tuple[ConvergenceStep, ...]] = (
    MappingProxyType({
        ConvergenceGroup.PHASE_ZERO: (
            _lazy_step("lib.slskd_transfers", "converge_slskd_orphans", _SLSKD_ORPHAN_FAILURE),
            _lazy_step("lib.slskd_transfers", "reap_disk_orphans", _DISK_REAP_FAILURE),
            _lazy_step("lib.slskd_searches", "converge_slskd_searches", _SEARCH_LEDGER_FAILURE),
            _lazy_step("lib.slskd_transfer_ledger", "prune_transfer_ledger_cycle", _TRANSFER_LEDGER_FAILURE),
        ),
        ConvergenceGroup.END_OF_CYCLE: (
            _lazy_step("lib.download", "harvest_terminal_transfer_evidence", _HARVEST_FAILURE),
            _lazy_step("lib.slskd_transfers", "purge_completed_transfers", _COMPLETED_PURGE_FAILURE),
        ),
    })
)


def run_convergence_steps(
    ctx: CratediggerContext,
    steps: Sequence[ConvergenceStep],
    *,
    log: ExceptionLogger = logger,
) -> None:
    """Attempt every step in order, isolating each failure from the cycle."""
    for step in steps:
        try:
            step.run(ctx)
        except Exception:
            log.exception(step.failure_message)


def run_convergence_group(
    ctx: CratediggerContext,
    group: ConvergenceGroup,
    *,
    log: ExceptionLogger = logger,
) -> None:
    """Run one registered convergence group in its declared order."""
    run_convergence_steps(ctx, CONVERGENCE_STEPS[group], log=log)
