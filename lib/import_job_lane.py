"""The two claim lanes over one ``import_jobs`` row.

One row is claimed twice in its life, by two different workers, at two
different stages — and until this module existed the lane was a naming
convention rather than a value, so the eight claim statements (four routes
x two lanes) were eight hand-written copies of one transaction, differing
only in the two things a ``JobLane`` now says. They stayed in step by
docstring prose, which is how issue #1176 PR3 came to need the same fix
twice.

Scope, precisely: a lane describes the CLAIM. The other ``preview_*``
writers — the evidence-ready mark, the requeues, the heartbeats — still
spell their own columns, deliberately, because what they write is not one
shape wearing two prefixes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from lib.import_queue import (
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_WAITING,
)

#: How a claim writes each of ``JobLane.claim_columns``, positionally: row
#: *i* here is the assignment for column *i* there. Two ordered lists rather
#: than one mapping so the lane's fields stay ordinary attribute reads (a
#: reflective lookup would hide them from the dead-code sweep), and
#: ``JobLane.__post_init__`` refuses any lane whose column count disagrees
#: with this — so a lane that grows a sixth stamped column cannot silently
#: grow it in only one of the two places.
CLAIM_ASSIGNMENT_TEMPLATES: Final[tuple[str, ...]] = (
    "{column} = 'running'",
    "{column} = {column} + 1",
    "{column} = %s",
    "{column} = COALESCE({column}, NOW())",
    "{column} = NOW()",
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

    Both the production claim statements and ``FakePipelineDB``'s claim read
    their columns from this one value, so the CLAIM cannot drift apart
    between the two lanes or between production and the fake. It says
    nothing about the lanes' other writers, which still spell their own
    columns (see the module docstring).
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

    def __post_init__(self) -> None:
        if len(self.claim_columns) != len(CLAIM_ASSIGNMENT_TEMPLATES):
            raise ValueError(
                "a lane's claim columns and their assignment templates must "
                f"pair one to one: {len(self.claim_columns)} columns against "
                f"{len(CLAIM_ASSIGNMENT_TEMPLATES)} templates"
            )

    @property
    def claim_columns(self) -> tuple[str, ...]:
        """The columns a claim writes, in ``CLAIM_ASSIGNMENT_TEMPLATES`` order."""
        return (
            self.status_column,
            self.attempts_column,
            self.worker_id_column,
            self.started_at_column,
            self.heartbeat_at_column,
        )

    @property
    def stamped_columns(self) -> tuple[str, ...]:
        """Every ``import_jobs`` column one claim in this lane writes.

        Excludes ``updated_at`` and the execution-lease columns, which both
        lanes write identically and which therefore say nothing about lane
        identity.

        Derived from ``claim_columns`` rather than re-listed, so this and the
        rendered SQL can never disagree about which columns a claim touches.
        """
        return (*self.claim_columns, *self.cleared_columns)


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
