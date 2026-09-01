"""Generated real-PostgreSQL patrol of the two ``import_jobs`` claim lanes.

The preview lane and the import lane used to be two hand-written copies of
one claim transaction, kept in step by docstring prose — issue #1176 PR3 had
to land the same routing fix in both halves. They are now one lane-taking
implementation over ``lib/import_job_lane.py``'s two values, and this file is
the patrol that keeps them one: it drives the real ``PipelineDB`` claim and
candidate-scan methods against real PostgreSQL for both lanes over the same
generated worlds.
"""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from typing import Literal

from hypothesis import assume, example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401  (boots and migrates ephemeral PostgreSQL)

from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from lib.import_job_lane import IMPORT_LANE, JOB_LANES, PREVIEW_LANE, JobLane
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
)
from lib.pipeline_db import PipelineDB
from lib.quality import ActiveDownloadState
from tests.dispatch_helpers import handoff_automation_owner
from tests.helpers import REQUEST_CASCADE_RESET_TABLES, delete_all_rows

TEST_DSN = os.environ["TEST_DB_DSN"]

RequestState = Literal[
    "current",
    "status_drifted",
    "processing_owned",
    "replaced",
    "replaced_before_enqueue",
]
PriorAttempt = Literal["none", "preview_restart", "import_requeued"]
# ``processing`` without an owner pointer is unreachable — migration 066's
# ``album_requests_processing_owner_equivalent`` CHECK rejects the UPDATE
# (CLAUDE.md invariant 10), so the representable refusal is a claim that
# names the wrong request for an intact owner.
OwnerState = Literal["exact", "wrong_request"]

# Per-clause proof (#1094): every clause of ``lane_claim_violations`` is named
# here so its self-test anchors on that clause's own message.
CLAUSE_CLAIMED_WRONG_LANE = (
    "a claim took a row parked in the other lane"
)
CLAUSE_CLAIMED_GUARDED_REQUEST = (
    "a claim took a row its request guard refuses"
)
CLAUSE_REFUSED_ITS_OWN_LANE = (
    "a claim refused a row its own lane and guard admit"
)
CLAUSE_OTHER_LANE_MUTATED = (
    "a claim mutated a column belonging to the other lane"
)
CLAUSE_LANE_NOT_STAMPED = (
    "a successful claim did not stamp its own lane"
)
CLAUSE_CLEARED_COLUMN_SURVIVED = (
    "a successful claim left its lane's cleared columns set"
)
CLAUSE_GUARDED_CANDIDATE_VISIBLE = (
    "a candidate scan offered a row its request guard refuses"
)
CLAUSE_ADMISSIBLE_CANDIDATE_INVISIBLE = (
    "a candidate scan hid a row its own lane admits"
)

#: One read of every claim column on an ``import_jobs`` row.
ColumnSnapshot = dict[str, object]

_LANES_BY_NAME = {lane.name: lane for lane in JOB_LANES}
#: Every column either lane's claim writes, read back to prove isolation.
_LANE_COLUMNS = (
    *(column for lane in JOB_LANES for column in lane.stamped_columns),
    # Not a lane column — both lanes write it identically — but every claim
    # must advance it, and nothing else in the suite asserts that
    # (issue #1313 review, mutant 17: dropping it from the unguarded claim
    # survived every other test).
    "updated_at",
)


def _attempts(columns: ColumnSnapshot, lane: JobLane) -> int:
    """One lane's attempt counter, or ``-1`` when the column is not an int.

    ``-1`` never equals ``before + 1`` for a non-negative counter, so a
    non-integer counter reads as "not stamped" rather than crashing the
    checker on a shape the schema forbids anyway.
    """
    value = columns[lane.attempts_column]
    return value if isinstance(value, int) else -1


@dataclass(frozen=True)
class LaneWorld:
    """One seeded claim world, as the test actually built it."""

    job_type: str
    #: The lane whose public claim method the test called.
    claim_lane: str
    #: The lane the seeded row was parked in.
    row_lane: str
    #: Whether the request guard admits THIS claim call. Derived from the
    #: state the test really produced, not from the claim's answer.
    guard_admits: bool
    #: Whether the shared routing table should offer this row in
    #: ``claim_lane``'s candidate scan. Distinct from ``claimable``: a scan
    #: answers "is this row on offer for its own request", while a claim call
    #: may still name the wrong request for it.
    scan_admits: bool

    @property
    def claimable(self) -> bool:
        return self.claim_lane == self.row_lane and self.guard_admits


def lane_claim_violations(
    world: LaneWorld,
    *,
    claimed: bool,
    columns_before: ColumnSnapshot,
    columns_after: ColumnSnapshot,
    visible_in_claim_lane_scan: bool,
) -> list[str]:
    """Every way the two lanes could stop being one claim.

    Accumulating rather than short-circuiting: a world can violate several
    clauses at once, and a ``raise`` chain would only ever prove the first.
    """
    violations: list[str] = []
    lane = _LANES_BY_NAME[world.claim_lane]
    other = _LANES_BY_NAME[
        IMPORT_LANE.name if lane is PREVIEW_LANE else PREVIEW_LANE.name
    ]

    if claimed and world.claim_lane != world.row_lane:
        violations.append(CLAUSE_CLAIMED_WRONG_LANE)
    if claimed and not world.guard_admits:
        violations.append(CLAUSE_CLAIMED_GUARDED_REQUEST)
    if not claimed and world.claimable:
        violations.append(CLAUSE_REFUSED_ITS_OWN_LANE)

    for column in other.stamped_columns:
        if columns_before[column] != columns_after[column]:
            violations.append(CLAUSE_OTHER_LANE_MUTATED)
            break

    if claimed:
        stamped = (
            columns_after[lane.status_column] == "running"
            and _attempts(columns_after, lane) == _attempts(columns_before, lane) + 1
            and columns_after[lane.heartbeat_at_column] is not None
            and columns_after[lane.started_at_column] is not None
            # COALESCE: a re-claim never re-dates the first claim.
            and (
                columns_before[lane.started_at_column] is None
                or columns_after[lane.started_at_column]
                == columns_before[lane.started_at_column]
            )
            # Every claim advances the queue-timeline clock.
            and columns_after["updated_at"]
            != columns_before["updated_at"]
        )
        if not stamped:
            violations.append(CLAUSE_LANE_NOT_STAMPED)
        if any(
            columns_after[column] is not None
            for column in lane.cleared_columns
        ):
            violations.append(CLAUSE_CLEARED_COLUMN_SURVIVED)

    # The scan is read BEFORE the claim, so it answers "was this row on
    # offer", never "did the claim consume it".
    if visible_in_claim_lane_scan and not world.scan_admits:
        violations.append(CLAUSE_GUARDED_CANDIDATE_VISIBLE)
    if world.scan_admits and not visible_in_claim_lane_scan:
        violations.append(CLAUSE_ADMISSIBLE_CANDIDATE_INVISIBLE)
    return violations


def _lease(unit: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="boot-lane-parity",
        invocation_id=f"invocation-{unit}",
        systemd_unit=f"cratedigger-{unit}.service",
        worker=ProcessIdentity(pid=4242, start_ticks=424242),
    )


class TestImportJobLaneParityGenerated(unittest.TestCase):
    def setUp(self) -> None:
        self.db = PipelineDB(TEST_DSN)

    def tearDown(self) -> None:
        self.db.close()

    def _reset(self) -> None:
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)

    def _request(self, key: str) -> int:
        return self.db.add_request(
            "Lane Artist",
            "Lane Album",
            "request",
            mb_release_id=key,
        )

    def _supersede(self, request_id: int) -> None:
        """Freeze one request through the sole producer of ``replaced``."""
        self.db.supersede_request_mbid(
            request_id,
            new_mb_release_id=f"lane-parity-successor-{request_id}",
            new_mb_release_group_id=None,
            new_mb_artist_id=None,
            new_artist_name="Lane Artist",
            new_album_title="Lane Album",
            new_year=None,
            new_country=None,
            new_tracks=[],
        )

    def _payload(self, job_type: str, request_id: int) -> ColumnSnapshot:
        if job_type == IMPORT_JOB_FORCE:
            return {"download_log_id": 1, "failed_path": "/quarantine/album"}
        if job_type == IMPORT_JOB_LOCAL:
            return {"source_path": "/operator/album", "request_id": request_id}
        return {
            "staged_path": "/incoming/auto-import/album",
            "request_id": request_id,
            "browse_id": "MPREb_lane",
            "download_log_id": 1,
        }

    def _preview_claim(self, job: ImportJob, worker_id: str) -> object:
        request_id = job.request_id or 0
        if job.job_type == IMPORT_JOB_YOUTUBE:
            return self.db.claim_import_preview_job_candidate(
                job.id, worker_id=worker_id,
            )
        if job.job_type == IMPORT_JOB_FORCE:
            return self.db.claim_force_import_preview_job_under_lock(
                job.id, request_id=request_id, worker_id=worker_id,
            )
        return self.db.claim_local_import_preview_job_under_lock(
            job.id, request_id=request_id, worker_id=worker_id,
        )

    def _leave_a_prior_attempt(
        self, job: ImportJob, prior_attempt: PriorAttempt,
    ) -> None:
        """Replay a real earlier attempt so the row is not pristine.

        Without this the claim always sees a freshly enqueued row, where the
        other lane's columns are all at their defaults — so a claim that
        wrongly wrote a default value into them would change nothing
        observable, and the preview lane's ``cleared_columns`` would never
        have anything to clear.

        ``preview_restart``: the worker-restart sequence — claim the preview,
        then let startup recovery requeue the running row, which is the only
        producer of a ``waiting`` row whose ``preview_message`` is set.
        ``import_requeued``: the importer's own missing-evidence requeue,
        which returns the row to ``waiting`` with ``attempts`` preserved.
        """
        if prior_attempt == "none":
            return
        if prior_attempt == "preview_restart":
            self.assertIsNotNone(
                self._preview_claim(job, "prior-preview-worker"),
                "prior preview claim refused",
            )
            requeued = self.db.requeue_running_import_preview_jobs(
                message="worker restarted mid-preview",
            )
            self.assertEqual([row.id for row in requeued], [job.id])
            return
        self._park_in_lane(job, IMPORT_LANE)
        self.assertTrue(self._claim(job, IMPORT_LANE), "prior import claim")
        self.assertIsNotNone(
            self.db.requeue_import_job_for_preview(
                job.id, reason="candidate evidence went stale",
            ),
            "prior import requeue refused",
        )

    def _park_in_lane(self, job: ImportJob, lane: JobLane) -> None:
        """Move a ``waiting`` row into ``lane`` through its real producers.

        The import lane is only ever entered by a preview claim followed by
        the evidence-ready mark, so the parked row carries a real preview
        attempt's columns rather than their insert defaults.
        """
        if lane is PREVIEW_LANE:
            return  # enqueue_import_job already parks rows at 'waiting'
        self.assertIsNotNone(
            self._preview_claim(job, "parking-preview-worker"),
            "parking preview claim refused",
        )
        moved = self.db.mark_import_job_preview_importable(
            # A message the import-lane claim must leave alone: without it
            # every preview column is already NULL in an import-claimable
            # world and CLAUSE_OTHER_LANE_MUTATED has nothing to observe.
            job.id, message="parked for import",
        )
        self.assertIsNotNone(moved, "preview-importable producer refused")

    def _columns(self, job_id: int) -> ColumnSnapshot:
        cur = self.db._execute(
            f"SELECT {', '.join(_LANE_COLUMNS)} FROM import_jobs WHERE id = %s",
            (job_id,),
        )
        row = cur.fetchone()
        assert row is not None
        return {column: row[column] for column in _LANE_COLUMNS}

    def _scan_ids(self, lane: JobLane) -> set[int]:
        lease = _lease("importer")
        if lane is PREVIEW_LANE:
            candidates = self.db.peek_import_preview_job_candidates(
                execution_lease=lease, limit=50,
            )
        else:
            candidates = self.db.peek_import_job_candidates(
                execution_lease=lease, limit=50,
            )
        return {candidate.id for candidate in candidates}

    def _claim(self, job: ImportJob, lane: JobLane) -> bool:
        request_id = job.request_id
        assert request_id is not None
        if job.job_type == IMPORT_JOB_YOUTUBE:
            claimed = (
                self.db.claim_import_preview_job_candidate(
                    job.id, worker_id="lane-worker",
                )
                if lane is PREVIEW_LANE
                else self.db.claim_import_job_candidate(
                    job.id, worker_id="lane-worker",
                )
            )
        elif job.job_type == IMPORT_JOB_FORCE:
            claimed = (
                self.db.claim_force_import_preview_job_under_lock(
                    job.id, request_id=request_id, worker_id="lane-worker",
                )
                if lane is PREVIEW_LANE
                else self.db.claim_force_import_job_under_lock(
                    job.id, request_id=request_id, worker_id="lane-worker",
                )
            )
        else:
            claimed = (
                self.db.claim_local_import_preview_job_under_lock(
                    job.id, request_id=request_id, worker_id="lane-worker",
                )
                if lane is PREVIEW_LANE
                else self.db.claim_local_import_job_under_lock(
                    job.id, request_id=request_id, worker_id="lane-worker",
                )
            )
        return claimed is not None

    # The #1176 PR3 regression world, which no test covered before this file:
    # a request-scoped job whose request an automation owner already holds.
    @example(
        job_type=IMPORT_JOB_LOCAL,
        claim_lane="import",
        row_lane="import",
        request_state="processing_owned",
        prior_attempt="none",
        already_running=False,
    )
    @example(
        job_type=IMPORT_JOB_LOCAL,
        claim_lane="preview",
        row_lane="preview",
        request_state="processing_owned",
        prior_attempt="none",
        already_running=False,
    )
    # The only worlds that can fire CLAUSE_REFUSED_ITS_OWN_LANE and
    # CLAUSE_ADMISSIBLE_CANDIDATE_INVISIBLE: lane matches and the guard admits.
    @example(
        job_type=IMPORT_JOB_FORCE,
        claim_lane="import",
        row_lane="import",
        request_state="current",
        prior_attempt="none",
        already_running=False,
    )
    # The only world that can fire CLAUSE_CLEARED_COLUMN_SURVIVED: a preview
    # claim over a row a restart left carrying the previous attempt's message.
    @example(
        job_type=IMPORT_JOB_YOUTUBE,
        claim_lane="preview",
        row_lane="preview",
        request_state="current",
        prior_attempt="preview_restart",
        already_running=False,
    )
    # A preview claim over a row the importer handed back: ``attempts`` is
    # non-zero, so an import-lane column a preview claim wrongly reset is
    # observable rather than already at its default.
    @example(
        job_type=IMPORT_JOB_FORCE,
        claim_lane="preview",
        row_lane="preview",
        request_state="current",
        prior_attempt="import_requeued",
        already_running=False,
    )
    # A Replace superseded the request while its force job was still queued.
    @example(
        job_type=IMPORT_JOB_FORCE,
        claim_lane="import",
        row_lane="import",
        request_state="replaced",
        prior_attempt="none",
        already_running=False,
    )
    # The only world that isolates the guard's ``replaced`` clause: a job
    # enqueued against an already-frozen row, so its own
    # ``expected_request_status`` is ``replaced`` and matches. Nothing in the
    # product enqueues here today — the clause is fail-closed legislation, and
    # this world is what proves it is legislation rather than dead SQL.
    @example(
        job_type=IMPORT_JOB_FORCE,
        claim_lane="preview",
        row_lane="preview",
        request_state="replaced_before_enqueue",
        prior_attempt="none",
        already_running=False,
    )
    # The only worlds that isolate every claim's ``status = 'queued'``
    # conjunct: a row a worker already holds, offered to each lane in turn.
    @example(
        job_type=IMPORT_JOB_FORCE,
        claim_lane="import",
        row_lane="import",
        request_state="current",
        prior_attempt="none",
        already_running=True,
    )
    @example(
        job_type=IMPORT_JOB_YOUTUBE,
        claim_lane="preview",
        row_lane="import",
        request_state="current",
        prior_attempt="none",
        already_running=True,
    )
    @given(
        job_type=st.sampled_from(
            (IMPORT_JOB_FORCE, IMPORT_JOB_LOCAL, IMPORT_JOB_YOUTUBE),
        ),
        claim_lane=st.sampled_from(("preview", "import")),
        row_lane=st.sampled_from(("preview", "import")),
        request_state=st.sampled_from(
            (
                "current",
                "status_drifted",
                "processing_owned",
                "replaced",
                "replaced_before_enqueue",
            ),
        ),
        prior_attempt=st.sampled_from(
            ("none", "preview_restart", "import_requeued"),
        ),
        already_running=st.booleans(),
    )
    def test_real_pg_request_scoped_and_youtube_lanes_are_one_claim(
        self,
        job_type: str,
        claim_lane: str,
        row_lane: str,
        request_state: RequestState,
        prior_attempt: PriorAttempt,
        already_running: bool,
    ) -> None:
        # Only an import claim moves ``status`` off ``queued``, and only a row
        # parked in the import lane can take one.
        assume(not already_running or row_lane == "import")
        # A row enqueued against an already-frozen request can never reach
        # the import lane: parking it there needs a preview claim, and the
        # same guard refuses that too. The world exists in the preview lane
        # only, which is where the guard's ``replaced`` clause is observable.
        assume(
            request_state != "replaced_before_enqueue"
            or (row_lane == "preview" and prior_attempt == "none")
        )
        self._reset()
        request_id = self._request("lane-parity-request")
        if request_state == "replaced_before_enqueue":
            self._supersede(request_id)
        job = self.db.enqueue_import_job(
            job_type,
            request_id=request_id,
            payload=self._payload(job_type, request_id),
        )
        self._leave_a_prior_attempt(job, prior_attempt)
        self._park_in_lane(job, _LANES_BY_NAME[row_lane])
        if already_running:
            # A worker already holds this row. Nothing may claim it again in
            # either lane until a terminal write or a recovery releases it.
            self.assertTrue(
                self._claim(job, IMPORT_LANE), "pre-claim left the row queued",
            )

        if request_state == "status_drifted":
            self.assertTrue(
                self.db.set_downloading(
                    request_id,
                    ActiveDownloadState(
                        filetype="flac",
                        enqueued_at="2026-07-01T00:00:00+00:00",
                        files=[],
                    ).to_json(),
                ),
            )
        elif request_state == "processing_owned":
            handoff_automation_owner(self.db, request_id)
        elif request_state == "replaced":
            self._supersede(request_id)

        # YouTube claims are request-blind by design (#1176 PR3): the type is
        # named positively in the routing table precisely so it needs no
        # request guard of its own.
        guard_admits = (
            job_type == IMPORT_JOB_YOUTUBE or request_state == "current"
        )
        lane = _LANES_BY_NAME[claim_lane]
        before = self._columns(job.id)
        # The preview scan — and only the preview scan — holds a re-measured
        # row back for IMPORT_PREVIEW_REQUEUE_INITIAL_DELAY after any earlier
        # attempt. Every world here was built moments ago, so a non-zero
        # ``attempts`` means that one-minute window is still open. This is
        # the one deliberate asymmetry between the lanes' candidate scans;
        # the claim itself stays symmetric, which is why it is scoped to
        # ``scan_admits`` and not to ``guard_admits``.
        preview_backoff_pending = (
            lane is PREVIEW_LANE and _attempts(before, IMPORT_LANE) > 0
        )
        # Measured, not assumed: every claim and both scans gate on
        # ``status = 'queued'``, so a row a worker already holds is neither
        # claimable nor on offer, in either lane.
        row_is_running = before["status"] == "running"
        world = LaneWorld(
            job_type=job_type,
            claim_lane=claim_lane,
            row_lane=row_lane,
            guard_admits=guard_admits and not row_is_running,
            scan_admits=(
                claim_lane == row_lane
                and guard_admits
                and not row_is_running
                and not preview_backoff_pending
            ),
        )
        visible = job.id in self._scan_ids(lane)
        claimed = self._claim(job, lane)
        after = self._columns(job.id)

        self.assertEqual(
            lane_claim_violations(
                world,
                claimed=claimed,
                columns_before=before,
                columns_after=after,
                visible_in_claim_lane_scan=visible,
            ),
            [],
        )

    @example(claim_lane="preview", row_lane="preview", owner_state="exact")
    @example(claim_lane="import", row_lane="import", owner_state="exact")
    @given(
        claim_lane=st.sampled_from(("preview", "import")),
        row_lane=st.sampled_from(("preview", "import")),
        owner_state=st.sampled_from(("exact", "wrong_request")),
    )
    def test_real_pg_automation_owner_claims_the_same_way_in_both_lanes(
        self,
        claim_lane: str,
        row_lane: str,
        owner_state: OwnerState,
    ) -> None:
        self._reset()
        request_id = self._request("lane-parity-owner")
        # A second request mid-processing under its OWN owner: the wrong
        # request a concurrent claim could name is a live one, not an idle
        # row, so the guard cannot pass merely by finding some owner there.
        bystander_id = self._request("lane-parity-bystander")
        handoff_automation_owner(self.db, bystander_id)
        job = handoff_automation_owner(self.db, request_id)
        lease = _lease("import-preview-worker")
        if row_lane == "import":
            # The only production route into the import lane for an owner row
            # is its own preview claim followed by the evidence-ready mark.
            self.assertIsNotNone(
                self.db.claim_automation_import_preview_job_under_lock(
                    job.id,
                    request_id=request_id,
                    worker_id="lane-preview",
                    execution_lease=lease,
                ),
            )
            self.assertIsNotNone(
                self.db.mark_import_job_preview_importable(
                    job.id, expected_execution_lease=lease,
                ),
            )

        claim_request_id = (
            request_id if owner_state == "exact" else bystander_id
        )
        lane = _LANES_BY_NAME[claim_lane]
        before = self._columns(job.id)
        preview_backoff_pending = (
            lane is PREVIEW_LANE and _attempts(before, IMPORT_LANE) > 0
        )
        world = LaneWorld(
            job_type="automation_import",
            claim_lane=claim_lane,
            row_lane=row_lane,
            guard_admits=owner_state == "exact",
            # The owner pointer stays intact in both worlds, so the routing
            # table offers the row in its own lane either way.
            scan_admits=claim_lane == row_lane and not preview_backoff_pending,
        )
        visible = job.id in self._scan_ids(lane)
        claimed = (
            self.db.claim_automation_import_preview_job_under_lock(
                job.id,
                request_id=claim_request_id,
                worker_id="lane-worker",
                execution_lease=lease,
            )
            if lane is PREVIEW_LANE
            else self.db.claim_automation_import_job_under_lock(
                job.id,
                request_id=claim_request_id,
                worker_id="lane-worker",
                execution_lease=lease,
            )
        ) is not None
        after = self._columns(job.id)

        self.assertEqual(
            lane_claim_violations(
                world,
                claimed=claimed,
                columns_before=before,
                columns_after=after,
                visible_in_claim_lane_scan=visible,
            ),
            [],
        )


def _columns(**overrides: object) -> ColumnSnapshot:
    """A claim-column snapshot for the checker's own self-tests."""
    snapshot: ColumnSnapshot = {
        "status": "queued",
        "attempts": 0,
        "worker_id": None,
        "started_at": None,
        "heartbeat_at": None,
        "preview_status": "waiting",
        "preview_attempts": 0,
        "preview_worker_id": None,
        "preview_started_at": None,
        "preview_heartbeat_at": None,
        "preview_message": None,
        "preview_error": None,
        "updated_at": "2026-09-01T00:00:00+00:00",
    }
    snapshot.update(overrides)
    return snapshot


def _stamped(lane: JobLane, base: ColumnSnapshot) -> ColumnSnapshot:
    """The snapshot a correct claim in ``lane`` produces from ``base``."""
    after = dict(base)
    after[lane.status_column] = "running"
    after[lane.attempts_column] = _attempts(base, lane) + 1
    after[lane.worker_id_column] = "lane-worker"
    after[lane.started_at_column] = "2026-09-01T00:00:00+00:00"
    after[lane.heartbeat_at_column] = "2026-09-01T00:00:01+00:00"
    after["updated_at"] = "2026-09-01T00:00:01+00:00"
    for column in lane.cleared_columns:
        after[column] = None
    return after


@dataclass(frozen=True)
class _ClauseCase:
    """The minimal world that makes exactly one checker clause fire."""

    description: str
    world: LaneWorld
    claimed: bool
    columns_before: ColumnSnapshot
    columns_after: ColumnSnapshot
    visible: bool
    message: str


class TestLaneClaimClausesTripOnViolations(unittest.TestCase):
    """One named world per clause, anchored on that clause's own message."""

    def test_each_clause_has_a_world_that_fires_only_it(self) -> None:
        clean = _columns()
        preview_ok = _stamped(PREVIEW_LANE, clean)
        cases: tuple[_ClauseCase, ...] = (
            _ClauseCase(
                "claimed a row parked in the import lane from the preview lane",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "import",
                    guard_admits=True, scan_admits=False,
                ),
                claimed=True,
                columns_before=clean,
                columns_after=preview_ok,
                # Hidden: a wrong-lane row is not claimable, so an offered
                # one would fire the visibility clause too.
                visible=False,
                message=CLAUSE_CLAIMED_WRONG_LANE,
            ),
            _ClauseCase(
                "claimed although the request guard refuses",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "preview",
                    guard_admits=False, scan_admits=False,
                ),
                claimed=True,
                columns_before=clean,
                columns_after=preview_ok,
                visible=False,
                message=CLAUSE_CLAIMED_GUARDED_REQUEST,
            ),
            _ClauseCase(
                "refused a row its own lane and guard admit",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "preview",
                    guard_admits=True, scan_admits=True,
                ),
                claimed=False,
                columns_before=clean,
                columns_after=clean,
                visible=True,
                message=CLAUSE_REFUSED_ITS_OWN_LANE,
            ),
            _ClauseCase(
                "a preview claim moved the import lane's status",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "preview",
                    guard_admits=True, scan_admits=True,
                ),
                claimed=True,
                columns_before=clean,
                columns_after={**preview_ok, "status": "running"},
                visible=True,
                message=CLAUSE_OTHER_LANE_MUTATED,
            ),
            _ClauseCase(
                "a successful preview claim left its own lane queued",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "preview",
                    guard_admits=True, scan_admits=True,
                ),
                claimed=True,
                columns_before=clean,
                columns_after=clean,
                visible=True,
                message=CLAUSE_LANE_NOT_STAMPED,
            ),
            _ClauseCase(
                "a successful preview claim kept the previous attempt's error",
                LaneWorld(
                    IMPORT_JOB_FORCE, "preview", "preview",
                    guard_admits=True, scan_admits=True,
                ),
                claimed=True,
                columns_before=clean,
                columns_after={**preview_ok, "preview_error": "stale failure"},
                visible=True,
                message=CLAUSE_CLEARED_COLUMN_SURVIVED,
            ),
            _ClauseCase(
                "the scan offered a row whose request guard refuses it",
                LaneWorld(
                    IMPORT_JOB_LOCAL, "preview", "preview",
                    guard_admits=False, scan_admits=False,
                ),
                claimed=False,
                columns_before=clean,
                columns_after=clean,
                visible=True,
                message=CLAUSE_GUARDED_CANDIDATE_VISIBLE,
            ),
            _ClauseCase(
                "the scan hid a row its own lane admits",
                LaneWorld(
                    IMPORT_JOB_LOCAL, "preview", "preview",
                    guard_admits=True, scan_admits=True,
                ),
                claimed=True,
                columns_before=clean,
                columns_after=preview_ok,
                visible=False,
                message=CLAUSE_ADMISSIBLE_CANDIDATE_INVISIBLE,
            ),
        )
        for case in cases:
            with self.subTest(case.description):
                violations = lane_claim_violations(
                    case.world,
                    claimed=case.claimed,
                    columns_before=case.columns_before,
                    columns_after=case.columns_after,
                    visible_in_claim_lane_scan=case.visible,
                )
                self.assertEqual(
                    violations,
                    [case.message],
                    f"{case.description!r} must fire exactly its own clause",
                )

    def test_a_correct_claim_in_either_lane_fires_nothing(self) -> None:
        """Must-still-work: no clause fires on a correctly claimed world."""
        clean = _columns()
        for lane in JOB_LANES:
            with self.subTest(lane=lane.name):
                self.assertEqual(
                    lane_claim_violations(
                        LaneWorld(
                            IMPORT_JOB_FORCE, lane.name, lane.name,
                            guard_admits=True, scan_admits=True,
                        ),
                        claimed=True,
                        columns_before=clean,
                        columns_after=_stamped(lane, clean),
                        visible_in_claim_lane_scan=True,
                    ),
                    [],
                )

    def test_a_correctly_refused_guarded_world_fires_nothing(self) -> None:
        """Must-still-work: a guard-refused, scan-hidden world is clean."""
        clean = _columns()
        self.assertEqual(
            lane_claim_violations(
                LaneWorld(
                    IMPORT_JOB_LOCAL, "import", "import",
                    guard_admits=False, scan_admits=False,
                ),
                claimed=False,
                columns_before=clean,
                columns_after=clean,
                visible_in_claim_lane_scan=False,
            ),
            [],
        )

    def test_clause_messages_are_distinct(self) -> None:
        messages = (
            CLAUSE_CLAIMED_WRONG_LANE,
            CLAUSE_CLAIMED_GUARDED_REQUEST,
            CLAUSE_REFUSED_ITS_OWN_LANE,
            CLAUSE_OTHER_LANE_MUTATED,
            CLAUSE_LANE_NOT_STAMPED,
            CLAUSE_CLEARED_COLUMN_SURVIVED,
            CLAUSE_GUARDED_CANDIDATE_VISIBLE,
            CLAUSE_ADMISSIBLE_CANDIDATE_INVISIBLE,
        )
        self.assertEqual(len(set(messages)), len(messages))


if __name__ == "__main__":
    unittest.main()
