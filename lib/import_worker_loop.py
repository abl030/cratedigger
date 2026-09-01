"""What the two import-queue workers share about draining a queue.

``scripts/importer.py`` and ``scripts/import_preview_worker.py`` claim from
the same ``import_jobs`` table at the two stages ``lib/import_job_lane.py``
names. What they do with a claim is genuinely different — one runs Beets,
the other measures evidence — but how they *reach* one is the same shape,
and it lived here twice: the same bounded-scan cursor, the same
peek-then-wrap-then-claim skeleton, the same lease reconstruction, the same
claim marker.

Nothing here knows what a claim is FOR, which is why the executing halves
of both workers stay where they are. One member is not shared:
``GracefulShutdown`` has a single user (the importer owns the only SIGTERM
handler), and it lives here beside the loop it stops rather than in the
worker that installs it.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ProcessIdentity,
    capture_execution_lease,
)
from lib.import_queue import ImportJob

logger = logging.getLogger("cratedigger")


@dataclass
class ClaimState:
    """Whether this poll pass took ownership of the candidate it tried.

    A claim route returns ``None`` both when it never got the row and when it
    got the row and the work produced nothing, so the return value alone
    cannot drive the scan cursor. The route marks this instead.
    """

    claimed: bool = False

    def mark(self) -> None:
        self.claimed = True


@dataclass
class CandidateScanCursor:
    """Where the next bounded candidate scan resumes.

    Held across polls by the worker's own loop, so a page of unclaimable
    candidates does not starve the rows behind it.
    """

    offset: int = 0


@dataclass
class GracefulShutdown:
    """Signal-safe stop flag: SIGTERM sets it, the poll loop reads it.

    The IMPORTER is the only worker that installs a handler for this; the
    preview worker has no SIGTERM handling at all, which is why this flag
    lives beside the shared loop rather than being shared itself.

    Best-effort drain (issue #1089): a deploy switch's SIGTERM no longer has
    to kill an in-flight import mid-flight. The importer's ``run_once`` never
    returns until its own at-most-one claimed job reaches a terminal write,
    so simply not calling it again once this flag is set already IS "let the
    in-flight job finish, then stop claiming new work" — no special
    interruption of a running job is needed or attempted. Past the unit's
    ``TimeoutStopSec``, systemd still SIGKILLs exactly as before; the
    recovery-side crash-debris removal in ``lib.automation_recovery_debris``
    is that world's safety net, not this one.
    """

    requested: bool = False

    def request(self, signum: int, frame: object) -> None:
        del signum, frame
        self.requested = True


def stage_dsn(db: object) -> str | None:
    """The DSN a pinned-session claim route opens its own connection on.

    Every such route needs one and refuses the candidate without it, in both
    workers — so the falsy-to-``None`` coercion is written once.
    """
    value = getattr(db, "dsn", None)
    return str(value) if value else None


def execution_lease_from_job(
    job: ImportJob | None,
) -> ExecutionLeaseSnapshot | None:
    """Rebuild the lease a persisted job row is carrying, if it carries one.

    Incomplete evidence is no evidence: a row missing any of the five worker
    fields yields ``None`` rather than a partial lease, so a liveness probe
    can never be handed something it would read as a match.
    """
    if job is None:
        return None
    values = (
        job.execution_invocation_id,
        job.execution_host_boot_id,
        job.execution_systemd_unit,
        job.execution_worker_pid,
        job.execution_worker_start_ticks,
    )
    if any(value is None for value in values):
        return None
    assert job.execution_invocation_id is not None
    assert job.execution_host_boot_id is not None
    assert job.execution_systemd_unit is not None
    assert job.execution_worker_pid is not None
    assert job.execution_worker_start_ticks is not None
    child = (
        ProcessIdentity(
            job.execution_beets_pid,
            job.execution_beets_start_ticks,
        )
        if (
            job.execution_beets_pid is not None
            and job.execution_beets_start_ticks is not None
        )
        else None
    )
    return ExecutionLeaseSnapshot(
        host_boot_id=job.execution_host_boot_id,
        invocation_id=job.execution_invocation_id,
        systemd_unit=job.execution_systemd_unit,
        worker=ProcessIdentity(
            job.execution_worker_pid,
            job.execution_worker_start_ticks,
        ),
        beets=child,
    )


def capture_worker_execution_lease(
    *,
    systemd_unit: str,
    factory: Callable[..., ExecutionLeaseSnapshot] | None = None,
) -> ExecutionLeaseSnapshot | None:
    """This worker's own lease, or ``None`` outside systemd.

    Non-systemd development runs may still process force/local-import/YouTube
    jobs; automation stays invisible to claim without a complete invocation
    lease, which is what the ``None`` produces downstream.
    """
    capture = factory or capture_execution_lease
    try:
        return capture(systemd_unit=systemd_unit)
    except ValueError:
        return None


def claim_one_candidate(
    *,
    scan_cursor: CandidateScanCursor,
    peek: Callable[[int], list[ImportJob]],
    claim: Callable[[ImportJob, ClaimState], ImportJob | None],
) -> ImportJob | None:
    """One bounded scan: offer a page of candidates, claim at most one.

    The cursor advances past a page that yielded nothing, and wraps to the
    head when a page comes back empty AND the cursor was not already there
    — so a run of unclaimable rows cannot hide the rows behind them, while
    an empty page at the head is not scanned twice. A successful claim
    resets it, because any success is a bounded revisit point for older
    rows that may now be claimable.
    """
    candidates = peek(scan_cursor.offset)
    if not candidates and scan_cursor.offset:
        scan_cursor.offset = 0
        candidates = peek(0)
    for candidate in candidates:
        claim_state = ClaimState()
        result = claim(candidate, claim_state)
        if not claim_state.claimed:
            continue
        scan_cursor.offset = 0
        return result
    scan_cursor.offset += len(candidates)
    return None
