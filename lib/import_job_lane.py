"""The two claim lanes over one ``import_jobs`` row.

One row is claimed twice in its life, by two different workers, at two
different stages — and until this module existed the lane was a naming
convention rather than a value, so every queue method, fake stub and worker
loop existed twice under a ``preview_`` prefix and stayed in step only by
docstring prose.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from lib.import_queue import (
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WAITING,
)


@dataclass(frozen=True)
class JobLane:
    """One of the two stages at which a worker claims an ``import_jobs`` row.

    The preview worker takes a row at ``preview_status='waiting'`` and stamps
    the ``preview_*`` columns; the importer takes the same row back at
    ``preview_status='evidence_ready'`` and stamps the unprefixed columns.
    Everything else about the two claims is one shape — the ``status='queued'``
    gate, the request guards, the lock ordering, the execution-lease stamp, and
    the positive ``job_type`` routing table — so a lane says only which stage
    is being taken and which columns that claim writes.

    Both the production queue methods and ``FakePipelineDB`` read their columns
    from this one value, so the two lanes cannot drift apart in either.
    """

    #: Operator-facing lane name; also the discriminator in test parameters.
    name: str
    #: The ``preview_status`` a row must hold to be claimable in this lane.
    entry_preview_status: str
    #: The column this lane moves to ``'running'``.
    status_column: str
    #: The attempt counter this lane increments.
    attempts_column: str
    #: The column that records which worker holds the claim.
    worker_id_column: str
    #: First-claim timestamp; preserved across re-claims via ``COALESCE``.
    started_at_column: str
    #: Liveness timestamp the lane's heartbeat refreshes.
    heartbeat_at_column: str
    #: Columns a fresh claim clears. The preview lane drops the previous
    #: attempt's operator-facing message/error; the import lane keeps
    #: ``message``/``error`` for its terminal writer, so it clears nothing.
    cleared_columns: tuple[str, ...]

    @property
    def stamped_columns(self) -> tuple[str, ...]:
        """Every ``import_jobs`` column one claim in this lane writes.

        Excludes ``updated_at`` and the execution-lease columns, which both
        lanes write identically and which therefore say nothing about lane
        identity.
        """
        return (
            self.status_column,
            self.attempts_column,
            self.worker_id_column,
            self.started_at_column,
            self.heartbeat_at_column,
            *self.cleared_columns,
        )


IMPORT_LANE: Final = JobLane(
    name="import",
    entry_preview_status=IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    status_column="status",
    attempts_column="attempts",
    worker_id_column="worker_id",
    started_at_column="started_at",
    heartbeat_at_column="heartbeat_at",
    cleared_columns=(),
)

PREVIEW_LANE: Final = JobLane(
    name="preview",
    entry_preview_status=IMPORT_JOB_PREVIEW_WAITING,
    status_column="preview_status",
    attempts_column="preview_attempts",
    worker_id_column="preview_worker_id",
    started_at_column="preview_started_at",
    heartbeat_at_column="preview_heartbeat_at",
    cleared_columns=("preview_message", "preview_error"),
)

#: Both lanes, in the order a row travels them.
JOB_LANES: Final = (PREVIEW_LANE, IMPORT_LANE)
