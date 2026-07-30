"""Generated end-to-end processing-owner lifecycle proof (#898).

The state machine drives the production-parity database commands and services
for the whole bounded lifecycle:

``wanted -> downloading -> processing -> {wanted, imported}``

It deliberately interleaves stale A/B download witnesses, exact and wrong job
IDs, preview/import execution death, automatic world-failure recovery, and
operator invalidators. A small independent oracle checks the ownership
predicate, handoff witness, and terminal all-or-none shape.

Profiles, promotion policy, and fault-injection qualification:
``docs/generated-testing.md``.
"""

from __future__ import annotations

import copy
import os
import unittest
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, replace
from typing import Literal, Never

from hypothesis import example, given
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

import tests._hypothesis_profiles  # noqa: F401 - load active profile
from lib import transitions
from lib.import_execution import (
    AutomationOwnerFailStop,
    CancellationToken,
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    OwnerSessionIdentity,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_RUNNING,
    IMPORT_JOB_PREVIEW_WAITING,
    force_import_payload,
)
from lib.pipeline_delete_service import delete_pipeline_request
from lib.quality import ActiveDownloadState
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    make_album_quality_evidence,
    make_request_row,
)

_REQUEST_ID = 898
_RELEASE_ID = "75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0"
_WITNESS_A = "2026-07-29T00:00:00+00:00"
_WITNESS_B = "2026-07-29T00:00:01+00:00"
_WITNESSES = (_WITNESS_A, _WITNESS_B)
_CANONICAL_PATH = "/tmp/cratedigger-generated-processing-owner-898"

LifecycleStatus = Literal[
    "wanted",
    "downloading",
    "processing",
    "imported",
]
LifecycleStage = Literal[
    "none",
    "preview_waiting",
    "preview_running",
    "preview_ready",
    "import_running",
]


@dataclass
class _LifecycleOracle:
    status: LifecycleStatus = "wanted"
    stage: LifecycleStage = "none"
    witness: str | None = None
    owner_job_id: int | None = None
    preview_lease: ExecutionLeaseSnapshot | None = None
    import_lease: ExecutionLeaseSnapshot | None = None
    launched: bool = False


@dataclass(frozen=True)
class _TerminalFacts:
    request_released: bool
    audit_present: bool
    job_terminal: bool
    cleanup_consumed: bool


ClaimLane = Literal["preview", "import"]


@dataclass(frozen=True)
class _ClaimProgressFacts:
    status: str
    preview_status: str | None
    attempts: int
    preview_attempts: int


def _claim_progress_facts(
    db: FakePipelineDB,
    job_id: int,
) -> _ClaimProgressFacts:
    job = db.get_import_job(job_id)
    assert job is not None
    return _ClaimProgressFacts(
        status=job.status,
        preview_status=job.preview_status,
        attempts=job.attempts,
        preview_attempts=job.preview_attempts,
    )


def _assert_lock_miss_then_progress(
    *,
    lane: ClaimLane,
    before: _ClaimProgressFacts,
    after_miss: _ClaimProgressFacts,
    after_progress: _ClaimProgressFacts,
) -> None:
    """A transient IMPORT miss creates no claim and the next poll advances."""
    if after_miss != before:
        raise AssertionError(
            f"{lane} lock miss created execution state: {after_miss!r}"
        )
    if lane == "preview":
        progressed = (
            after_progress.preview_attempts == before.preview_attempts + 1
            and after_progress.preview_status == IMPORT_JOB_PREVIEW_RUNNING
        )
    else:
        progressed = after_progress.attempts == before.attempts + 1
    if not progressed:
        raise AssertionError(
            f"{lane} did not progress after contention cleared: "
            f"{after_progress!r}"
        )


class _GeneratedStageSession:
    """Pinned stage-session stand-in around the stateful production-parity DB."""

    def __init__(self, db: FakePipelineDB, *, acquire: bool) -> None:
        self.db = db
        self.acquire = acquire
        self.pinned = False

    def __getattr__(self, name: str) -> object:
        return getattr(self.db, name)

    def get_import_job(self, job_id: int):
        return self.db.get_import_job(job_id)

    def get_request(self, request_id: int) -> Mapping[str, object] | None:
        return self.db.get_request(request_id)

    @contextmanager
    def _pin_owner_session(self, token: CancellationToken):
        token.raise_if_cancelled()
        self.pinned = True
        try:
            yield OwnerSessionIdentity(id(self), 898)
        finally:
            self.pinned = False

    @contextmanager
    def advisory_lock(self, namespace: int, key: int):
        from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT

        if not self.pinned:
            raise AssertionError("generated IMPORT lock used an unpinned session")
        if namespace != ADVISORY_LOCK_NAMESPACE_IMPORT or key != _REQUEST_ID:
            raise AssertionError(
                f"unexpected generated advisory lock {(namespace, key)!r}"
            )
        yield self.acquire

    def claim_automation_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ):
        return self.db.claim_automation_import_preview_job_under_lock(
            job_id,
            request_id=request_id,
            worker_id=worker_id,
            execution_lease=execution_lease,
        )

    def claim_automation_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
        execution_lease: ExecutionLeaseSnapshot,
    ):
        return self.db.claim_automation_import_job_under_lock(
            job_id,
            request_id=request_id,
            worker_id=worker_id,
            execution_lease=execution_lease,
        )

    def claim_force_import_preview_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ):
        return self.db.claim_force_import_preview_job_under_lock(
            job_id,
            request_id=request_id,
            worker_id=worker_id,
        )

    def claim_force_import_job_under_lock(
        self,
        job_id: int,
        *,
        request_id: int,
        worker_id: str | None,
    ):
        return self.db.claim_force_import_job_under_lock(
            job_id,
            request_id=request_id,
            worker_id=worker_id,
        )

    def close(self) -> None:
        return None


class _ChangedBootProbe:
    """A reproducible proof that the exact persisted execution is dead."""

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=f"{lease.host_boot_id}-successor",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


def _active_state(witness: str) -> ActiveDownloadState:
    return ActiveDownloadState(
        filetype="flac",
        enqueued_at=witness,
        files=[],
    )


def _execution_lease(
    lane: Literal["preview", "import"],
    job_id: int,
    generation: int,
) -> ExecutionLeaseSnapshot:
    lane_offset = 0 if lane == "preview" else 100
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-processing-boot",
        invocation_id=f"generated-{lane}-{job_id}-{generation}",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(
            pid=10_000 + lane_offset + generation,
            start_ticks=20_000 + lane_offset + generation,
        ),
    )


def _handoff_allowed(
    *,
    status: str,
    stored_witness: str | None,
    supplied_witness: str,
    owner_job_id: int | None,
) -> bool:
    """Independent minimal oracle for the sole lifecycle handoff."""
    return (
        status == "downloading"
        and stored_witness is not None
        and stored_witness == supplied_witness
        and owner_job_id is None
    )


def _exact_processing_owner(
    request: Mapping[str, object],
    job_id: int,
) -> bool:
    """Independent owner predicate: status and exact pointer are inseparable."""
    return (
        request.get("status") == "processing"
        and request.get("active_automation_import_job_id") == job_id
    )


def _assert_terminal_all_or_none(
    before: _TerminalFacts,
    after: _TerminalFacts,
) -> None:
    """A terminal write is either invisible or commits every durable fact."""
    if after == before:
        return
    if after != _TerminalFacts(True, True, True, True):
        raise AssertionError(f"partial terminal bundle: {after!r}")




def _mutant_owner_ignores_pointer(
    request: Mapping[str, object],
    _job_id: int,
) -> bool:
    return request.get("status") == "processing"


def _mutant_handoff_ignores_witness(
    *,
    status: str,
    _stored_witness: str | None,
    _supplied_witness: str,
    owner_job_id: int | None,
) -> bool:
    return status == "downloading" and owner_job_id is None


def _assert_force_owner_fence(
    before: _ClaimProgressFacts,
    after: _ClaimProgressFacts,
    *,
    effect_count: int,
) -> None:
    if after != before or effect_count:
        raise AssertionError(
            "owned request admitted stale force execution state or effects"
        )


def _database_snapshot(db: FakePipelineDB) -> object:
    """All state that an owner-conflicted command is forbidden to mutate."""
    return copy.deepcopy((
        db._requests,
        db._import_jobs,
        db._processing_cleanup_journals,
        db.download_logs,
        db.status_history,
    ))




def _seed_candidate(
    db: FakePipelineDB,
    *,
    job_id: int,
    execution_lease: ExecutionLeaseSnapshot,
) -> None:
    evidence = make_album_quality_evidence(
        mb_release_id=_RELEASE_ID,
        source_path=_CANONICAL_PATH,
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=_RELEASE_ID,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    if not db.set_import_job_candidate_evidence(
        job_id,
        persisted.id,
        expected_execution_lease=execution_lease,
    ):
        raise AssertionError("exact preview owner rejected candidate evidence")


def _new_owner_db(
    *,
    witness: str = _WITNESS_A,
) -> tuple[FakePipelineDB, int]:
    if os.path.lexists(_CANONICAL_PATH):
        raise AssertionError(
            f"refusing generated cleanup against existing {_CANONICAL_PATH}"
        )
    db = FakePipelineDB()
    db.seed_request(make_request_row(
        id=_REQUEST_ID,
        status="wanted",
        mb_release_id=_RELEASE_ID,
    ))
    if not db.set_downloading(
        _REQUEST_ID,
        _active_state(witness).to_json(),
        expected_status="wanted",
    ):
        raise AssertionError("generated fixture could not start download")
    handoff = db.handoff_automation_import(
        request_id=_REQUEST_ID,
        expected_enqueued_at=witness,
        canonical_path=_CANONICAL_PATH,
        message="generated processing owner",
    )
    if not handoff.committed or handoff.job is None:
        raise AssertionError(f"generated owner handoff failed: {handoff.outcome}")
    return db, handoff.job.id




class ProcessingLifecycleMachine(RuleBasedStateMachine):
    """Randomly interleave the real lifecycle commands against a small model."""

    def __init__(self) -> None:
        super().__init__()
        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=_REQUEST_ID,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        if os.path.lexists(_CANONICAL_PATH):
            raise AssertionError(
                "generated canonical path unexpectedly exists"
            )
        self.oracle = _LifecycleOracle()
        self.generation = 0

    def _current_job(self):
        job_id = self.oracle.owner_job_id
        return None if job_id is None else self.db.get_import_job(job_id)

    @precondition(lambda self: self.oracle.status == "wanted")
    @rule(witness=st.sampled_from(_WITNESSES))
    def begin_download(self, witness: str) -> None:
        self.assert_or_raise(self.db.set_downloading(
            _REQUEST_ID,
            _active_state(witness).to_json(),
            expected_status="wanted",
        ))
        self.oracle.status = "downloading"
        self.oracle.witness = witness

    @precondition(lambda self: self.oracle.status == "downloading")
    @rule(supplied_witness=st.sampled_from(_WITNESSES))
    def handoff_download(self, supplied_witness: str) -> None:
        expected = _handoff_allowed(
            status=self.oracle.status,
            stored_witness=self.oracle.witness,
            supplied_witness=supplied_witness,
            owner_job_id=self.oracle.owner_job_id,
        )
        before = _database_snapshot(self.db)
        result = self.db.handoff_automation_import(
            request_id=_REQUEST_ID,
            expected_enqueued_at=supplied_witness,
            canonical_path=_CANONICAL_PATH,
            message="generated lifecycle handoff",
        )
        if expected:
            self.assert_or_raise(result.committed and result.job is not None)
            assert result.job is not None
            self.oracle.status = "processing"
            self.oracle.stage = "preview_waiting"
            self.oracle.owner_job_id = result.job.id
            self.generation += 1
        else:
            self.assert_or_raise(result.outcome == "witness_mismatch")
            self.assert_or_raise(_database_snapshot(self.db) == before)

    @precondition(
        lambda self: self.oracle.stage == "preview_waiting"
        and self.oracle.owner_job_id is not None
    )
    @rule()
    def claim_preview(self) -> None:
        assert self.oracle.owner_job_id is not None
        lease = _execution_lease(
            "preview",
            self.oracle.owner_job_id,
            self.generation,
        )
        claimed = claim_next_import_preview_job(self.db, worker_id="generated-preview",
        execution_lease=lease,)
        self.assert_or_raise(
            claimed is not None and claimed.id == self.oracle.owner_job_id
        )
        self.oracle.preview_lease = lease
        self.oracle.stage = "preview_running"

    @precondition(
        lambda self: self.oracle.stage == "preview_running"
        and self.oracle.owner_job_id is not None
        and self.oracle.preview_lease is not None
    )
    @rule(exact_lease=st.booleans())
    def finish_preview(self, exact_lease: bool) -> None:
        assert self.oracle.owner_job_id is not None
        assert self.oracle.preview_lease is not None
        supplied = (
            self.oracle.preview_lease
            if exact_lease
            else replace(
                self.oracle.preview_lease,
                invocation_id="stale-preview-invocation",
            )
        )
        before = _database_snapshot(self.db)
        if exact_lease:
            _seed_candidate(
                self.db,
                job_id=self.oracle.owner_job_id,
                execution_lease=supplied,
            )
            before = _database_snapshot(self.db)
        result = self.db.mark_import_job_preview_importable(
            self.oracle.owner_job_id,
            preview_result={"ready": True},
            expected_execution_lease=supplied,
        )
        if exact_lease:
            self.assert_or_raise(result is not None)
            self.oracle.preview_lease = None
            self.oracle.stage = "preview_ready"
        else:
            self.assert_or_raise(result is None)
            self.assert_or_raise(_database_snapshot(self.db) == before)

    @precondition(
        lambda self: self.oracle.stage == "preview_running"
        and self.oracle.owner_job_id is not None
    )
    @rule()
    def recover_dead_preview(self) -> None:
        from scripts import import_preview_worker

        assert self.oracle.owner_job_id is not None
        recovered = import_preview_worker.recover_running_preview_jobs(
            self.db,  # pyright: ignore[reportArgumentType]
            liveness_probe=_ChangedBootProbe(),
        )
        self.assert_or_raise(
            [job.id for job in recovered] == [self.oracle.owner_job_id]
        )
        self.oracle.preview_lease = None
        self.oracle.stage = "preview_waiting"

    @precondition(
        lambda self: self.oracle.stage == "preview_ready"
        and self.oracle.owner_job_id is not None
    )
    @rule()
    def claim_import(self) -> None:
        assert self.oracle.owner_job_id is not None
        lease = _execution_lease(
            "import",
            self.oracle.owner_job_id,
            self.generation,
        )
        claimed = claim_next_import_job(self.db, worker_id="generated-import",
        execution_lease=lease,)
        self.assert_or_raise(
            claimed is not None and claimed.id == self.oracle.owner_job_id
        )
        self.oracle.import_lease = lease
        self.oracle.stage = "import_running"

    @precondition(
        lambda self: self.oracle.stage == "import_running"
        and not self.oracle.launched
        and self.oracle.owner_job_id is not None
        and self.oracle.import_lease is not None
    )
    @rule(authority=st.sampled_from(
        ("exact", "wrong_job", "wrong_path", "stale_lease"),
    ))
    def authorize_import(self, authority: str) -> None:
        assert self.oracle.owner_job_id is not None
        assert self.oracle.import_lease is not None
        job_id = (
            self.oracle.owner_job_id + 1000
            if authority == "wrong_job"
            else self.oracle.owner_job_id
        )
        source_path = (
            f"{_CANONICAL_PATH}-stale"
            if authority == "wrong_path"
            else _CANONICAL_PATH
        )
        lease = (
            replace(
                self.oracle.import_lease,
                invocation_id="stale-import-invocation",
            )
            if authority == "stale_lease"
            else self.oracle.import_lease
        )
        before = _database_snapshot(self.db)
        result = self.db.authorize_import_job_launch(
            job_id,
            request_id=_REQUEST_ID,
            release_id=_RELEASE_ID,
            source_path=source_path,
            expected_execution_lease=lease,
        )
        if authority == "exact":
            self.assert_or_raise(result is not None)
            self.oracle.launched = True
        else:
            self.assert_or_raise(result is None)
            self.assert_or_raise(_database_snapshot(self.db) == before)

    @precondition(
        lambda self: self.oracle.stage == "import_running"
        and self.oracle.launched
        and self.oracle.owner_job_id is not None
        and self.oracle.import_lease is not None
    )
    @rule(child_started=st.booleans())
    def recover_dead_import(self, child_started: bool) -> None:
        from scripts import importer

        assert self.oracle.owner_job_id is not None
        assert self.oracle.import_lease is not None
        if child_started:
            child = ProcessIdentity(
                pid=30_000 + self.generation,
                start_ticks=40_000 + self.generation,
            )
            recorded = self.db.record_import_job_beets_child(
                self.oracle.owner_job_id,
                expected_execution_lease=self.oracle.import_lease,
                beets_pid=child.pid,
                beets_start_ticks=child.start_ticks,
            )
            self.assert_or_raise(recorded is not None)
            self.oracle.import_lease = replace(
                self.oracle.import_lease,
                beets=child,
            )
        recovered = importer.recover_abandoned_automation_owners(
            self.db,
            liveness_probe=_ChangedBootProbe(),
        )
        self.assert_or_raise(
            [job.id for job in recovered] == [self.oracle.owner_job_id]
        )
        self.oracle.status = "wanted"
        self.oracle.stage = "none"
        self.oracle.witness = None
        self.oracle.owner_job_id = None
        self.oracle.preview_lease = None
        self.oracle.import_lease = None
        self.oracle.launched = False

    @precondition(
        lambda self: self.oracle.status == "processing"
        and self.oracle.owner_job_id is not None
    )
    @rule(action=st.sampled_from(("delete", "search_stop")))
    def operator_invalidator_is_zero_mutation(self, action: str) -> None:
        before = _database_snapshot(self.db)
        if action == "delete":
            result = delete_pipeline_request(self.db, _REQUEST_ID)
        else:
            result = transitions.finalize_operator_request(
                self.db,
                _REQUEST_ID,
                transitions.RequestTransition.to_unsearchable(
                    from_status="processing",
                ),
            )
        self.assert_or_raise(
            isinstance(result, transitions.TransitionConflict)
        )
        assert isinstance(result, transitions.TransitionConflict)
        self.assert_or_raise(
            result.kind
            == transitions.TransitionConflictKind.processing_locked
        )
        self.assert_or_raise(_database_snapshot(self.db) == before)

    @precondition(
        lambda self: self.oracle.status == "processing"
        and self.oracle.owner_job_id is not None
    )
    @rule(
        command=st.sampled_from((
            "preview_complete",
            "preview_heartbeat",
            "import_heartbeat",
            "launch",
        )),
        offset=st.integers(min_value=1, max_value=5),
    )
    def wrong_job_id_is_zero_mutation(
        self,
        command: str,
        offset: int,
    ) -> None:
        assert self.oracle.owner_job_id is not None
        wrong_job_id = self.oracle.owner_job_id + offset
        lease = (
            self.oracle.preview_lease
            or self.oracle.import_lease
            or _execution_lease("preview", wrong_job_id, self.generation + 1)
        )
        before = _database_snapshot(self.db)
        if command == "preview_complete":
            result: object = self.db.mark_import_job_preview_importable(
                wrong_job_id,
                preview_result={"wrong": True},
                expected_execution_lease=lease,
            )
        elif command == "preview_heartbeat":
            result = self.db.heartbeat_import_job_preview(
                wrong_job_id,
                expected_execution_lease=lease,
            )
        elif command == "import_heartbeat":
            result = self.db.heartbeat_import_job(
                wrong_job_id,
                expected_execution_lease=lease,
            )
        else:
            result = self.db.authorize_import_job_launch(
                wrong_job_id,
                request_id=_REQUEST_ID,
                release_id=_RELEASE_ID,
                source_path=_CANONICAL_PATH,
                expected_execution_lease=lease,
            )
        self.assert_or_raise(result is None or result is False)
        self.assert_or_raise(_database_snapshot(self.db) == before)

    @precondition(lambda self: self.oracle.status == "imported")
    @rule()
    def requeue_imported(self) -> None:
        result = transitions.finalize_operator_request(
            self.db,
            _REQUEST_ID,
            transitions.RequestTransition.to_wanted(
                from_status="imported",
            ),
        )
        self.assert_or_raise(
            isinstance(result, transitions.TransitionApplied)
        )
        self.oracle.status = "wanted"

    @invariant()
    def exact_owner_and_stage_match_oracle(self) -> None:
        request = self.db.request(_REQUEST_ID)
        self.assert_or_raise(request["status"] == self.oracle.status)
        self.assert_or_raise(
            _exact_processing_owner(
                request,
                self.oracle.owner_job_id or -1,
            )
            == (self.oracle.owner_job_id is not None)
        )
        if self.oracle.witness is None:
            self.assert_or_raise(
                request.get("active_download_state") is None
            )
        else:
            state = ActiveDownloadState.from_raw(
                request["active_download_state"]
            )
            self.assert_or_raise(state.enqueued_at == self.oracle.witness)
            if self.oracle.status == "processing":
                self.assert_or_raise(state.current_path == _CANONICAL_PATH)
        active = [
            job
            for job in self.db.list_import_jobs(limit=100)
            if job.request_id == _REQUEST_ID
            and job.status in IMPORT_JOB_ACTIVE_STATUSES
        ]
        if self.oracle.owner_job_id is None:
            self.assert_or_raise(not active)
            return
        self.assert_or_raise(
            [job.id for job in active] == [self.oracle.owner_job_id]
        )
        job = self._current_job()
        assert job is not None
        expected_stage = {
            "preview_waiting": ("queued", IMPORT_JOB_PREVIEW_WAITING),
            "preview_running": ("queued", IMPORT_JOB_PREVIEW_RUNNING),
            "preview_ready": (
                "queued",
                IMPORT_JOB_PREVIEW_EVIDENCE_READY,
            ),
            "import_running": (
                "running",
                IMPORT_JOB_PREVIEW_EVIDENCE_READY,
            ),
        }[self.oracle.stage]
        self.assert_or_raise(
            (job.status, job.preview_status) == expected_stage
        )

    @staticmethod
    def assert_or_raise(condition: bool) -> None:
        if not condition:
            raise AssertionError("generated lifecycle assertion failed")


class TestProcessingLifecycleGenerated(unittest.TestCase):
    @given(
        stored_witness=st.sampled_from(_WITNESSES),
        supplied_witness=st.sampled_from(_WITNESSES),
    )
    @example(
        stored_witness=_WITNESS_A,
        supplied_witness=_WITNESS_B,
    )
    def test_handoff_uses_the_exact_download_incarnation(
        self,
        stored_witness: str,
        supplied_witness: str,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_REQUEST_ID,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        self.assertTrue(db.set_downloading(
            _REQUEST_ID,
            _active_state(stored_witness).to_json(),
            expected_status="wanted",
        ))
        before = _database_snapshot(db)
        result = db.handoff_automation_import(
            request_id=_REQUEST_ID,
            expected_enqueued_at=supplied_witness,
            canonical_path=_CANONICAL_PATH,
            message="generated witness",
        )
        expected = _handoff_allowed(
            status="downloading",
            stored_witness=stored_witness,
            supplied_witness=supplied_witness,
            owner_job_id=None,
        )
        self.assertEqual(result.committed, expected)
        if not expected:
            self.assertEqual(_database_snapshot(db), before)

    @given(
        command=st.sampled_from((
            "preview_complete",
            "preview_heartbeat",
            "import_heartbeat",
            "launch",
        )),
        offset=st.integers(min_value=1, max_value=10),
    )
    @example(command="preview_complete", offset=1)
    def test_wrong_job_id_cannot_advance_an_exact_owner_command(
        self,
        command: str,
        offset: int,
    ) -> None:
        db, job_id = _new_owner_db()
        wrong_job_id = job_id + offset
        lease = _execution_lease("preview", wrong_job_id, 1)
        before = _database_snapshot(db)
        if command == "preview_complete":
            result: object = db.mark_import_job_preview_importable(
                wrong_job_id,
                preview_result={"wrong": True},
                expected_execution_lease=lease,
            )
        elif command == "preview_heartbeat":
            result = db.heartbeat_import_job_preview(
                wrong_job_id,
                expected_execution_lease=lease,
            )
        elif command == "import_heartbeat":
            result = db.heartbeat_import_job(
                wrong_job_id,
                expected_execution_lease=lease,
            )
        else:
            result = db.authorize_import_job_launch(
                wrong_job_id,
                request_id=_REQUEST_ID,
                release_id=_RELEASE_ID,
                source_path=_CANONICAL_PATH,
                expected_execution_lease=lease,
            )
        self.assertIn(result, (None, False))
        self.assertEqual(_database_snapshot(db), before)

    @given(
        lane=st.sampled_from(("preview", "import")),
        misses=st.integers(min_value=1, max_value=4),
    )
    @example(lane="import", misses=2)
    def test_transient_stage_lock_contention_stays_claimable(
        self,
        lane: ClaimLane,
        misses: int,
    ) -> None:
        from lib.dispatch import DispatchOutcome
        from scripts import import_preview_worker, importer

        db, job_id = _new_owner_db()
        setattr(db, "dsn", "postgresql://generated")  # noqa: B010
        if lane == "preview":
            lease = _execution_lease("preview", job_id, 1)
        else:
            preview_lease = _execution_lease("preview", job_id, 1)
            claimed_preview = claim_next_import_preview_job(db, worker_id="generated-setup-preview",
            execution_lease=preview_lease,)
            assert claimed_preview is not None
            assert db.mark_import_job_preview_importable(
                job_id,
                preview_result={"ready": True},
                expected_execution_lease=preview_lease,
            ) is not None
            lease = _execution_lease("import", job_id, 2)

        before = _claim_progress_facts(db, job_id)
        for generation in range(misses):
            if lane == "preview":
                result = import_preview_worker.run_once(
                    db,
                    worker_id=f"generated-preview-miss-{generation}",
                    stage_db_factory=lambda _dsn: _GeneratedStageSession(
                        db,
                        acquire=False,
                    ),
                    execution_lease_factory=lambda **_kwargs: lease,
                )
            else:
                result = importer.run_once(
                    db,  # pyright: ignore[reportArgumentType]
                    worker_id=f"generated-import-miss-{generation}",
                    stage_db_factory=lambda _dsn: _GeneratedStageSession(
                        db,
                        acquire=False,
                    ),
                    execution_lease_factory=lambda **_kwargs: lease,
                )
            self.assertIsNone(result)
            self.assertEqual(_claim_progress_facts(db, job_id), before)

        after_miss = _claim_progress_facts(db, job_id)
        if lane == "preview":
            with self.assertRaises(AutomationOwnerFailStop):
                import_preview_worker.run_once(
                    db,
                    worker_id="generated-preview-progress",
                    stage_db_factory=lambda _dsn: _GeneratedStageSession(
                        db,
                        acquire=True,
                    ),
                    execution_lease_factory=lambda **_kwargs: lease,
                )
            result = None
        else:
            result = importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="generated-import-progress",
                stage_db_factory=lambda _dsn: _GeneratedStageSession(
                    db,
                    acquire=True,
                ),
                execution_lease_factory=lambda **_kwargs: lease,
                execute_fn=lambda *_args, **_kwargs: DispatchOutcome(
                    False,
                    "generated prelaunch defer",
                    deferred=True,
                ),
            )
        self.assertIsNone(result)
        _assert_lock_miss_then_progress(
            lane=lane,
            before=before,
            after_miss=after_miss,
            after_progress=_claim_progress_facts(db, job_id),
        )

    @given(
        lane=st.sampled_from(("preview", "import")),
        polls=st.integers(min_value=1, max_value=4),
    )
    @example(lane="preview", polls=2)
    @example(lane="import", polls=2)
    def test_force_queued_before_owner_handoff_never_crosses_stage_boundary(
        self,
        lane: ClaimLane,
        polls: int,
    ) -> None:
        from lib.dispatch import DispatchOutcome
        from lib.import_preview import ImportPreviewResult
        from scripts import import_preview_worker, importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://generated")  # noqa: B010
        db.seed_request(make_request_row(
            id=_REQUEST_ID,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        self.assertTrue(db.set_downloading(
            _REQUEST_ID,
            _active_state(_WITNESS_A).to_json(),
            expected_status="wanted",
        ))
        force_job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=_REQUEST_ID,
            dedupe_key=f"generated-force-before-owner:{lane}",
            payload=force_import_payload(
                download_log_id=898,
                failed_path="/tmp/generated-force-before-owner",
            ),
        )
        if lane == "import":
            self.assertIsNotNone(db.mark_import_job_preview_importable(
                force_job.id,
                preview_result={"verdict": "evidence_ready"},
                message="generated force ready",
            ))
        before = _claim_progress_facts(db, force_job.id)
        effects: list[str] = []
        handoff_done = False

        def capture_without_systemd(**_kwargs: object) -> Never:
            raise ValueError("generated force worker has no systemd lease")

        def stage_factory(_dsn: str) -> _GeneratedStageSession:
            nonlocal handoff_done
            if not handoff_done:
                handoff = db.handoff_automation_import(
                    request_id=_REQUEST_ID,
                    expected_enqueued_at=_WITNESS_A,
                    canonical_path=_CANONICAL_PATH,
                    message="generated owner wins after force selection",
                )
                if not handoff.committed:
                    raise AssertionError(
                        f"generated handoff failed: {handoff.outcome}"
                    )
                handoff_done = True
            return _GeneratedStageSession(db, acquire=True)

        def forbidden_preview(
            *_args: object,
            **_kwargs: object,
        ) -> ImportPreviewResult:
            effects.append("preview")
            raise AssertionError("stale force preview crossed owner fence")

        def forbidden_import(
            *_args: object,
            **_kwargs: object,
        ) -> DispatchOutcome:
            effects.append("import")
            raise AssertionError("stale force import crossed owner fence")

        for poll in range(polls):
            if lane == "preview":
                result = import_preview_worker.run_once(
                    db,
                    worker_id=f"generated-force-preview-{poll}",
                    stage_db_factory=stage_factory,
                    execution_lease_factory=capture_without_systemd,
                    candidate_measurement_fn=forbidden_preview,
                )
            else:
                result = importer.run_once(
                    db,  # pyright: ignore[reportArgumentType]
                    worker_id=f"generated-force-import-{poll}",
                    stage_db_factory=stage_factory,
                    execution_lease_factory=capture_without_systemd,
                    execute_fn=forbidden_import,
                )
            self.assertIsNone(result)
            _assert_force_owner_fence(
                before,
                _claim_progress_facts(db, force_job.id),
                effect_count=len(effects),
            )
        self.assertTrue(handoff_done)

    @given(
        lane=st.sampled_from(("preview", "import")),
        request_changed=st.booleans(),
    )
    @example(lane="preview", request_changed=False)
    @example(lane="import", request_changed=True)
    def test_force_lock_miss_preserves_exact_future_claimability(
        self,
        lane: ClaimLane,
        request_changed: bool,
    ) -> None:
        """A miss is zero-state; only the enqueue-time request state may retry."""
        from lib.dispatch import DispatchOutcome
        from lib.import_preview import ImportPreviewResult
        from scripts import import_preview_worker, importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://generated")  # noqa: B010
        db.seed_request(make_request_row(
            id=_REQUEST_ID,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        force_job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=_REQUEST_ID,
            dedupe_key=f"generated-force-lock-miss:{lane}",
            payload=force_import_payload(
                download_log_id=899,
                failed_path="/tmp/generated-force-lock-miss",
            ),
        )
        if lane == "import":
            self.assertIsNotNone(db.mark_import_job_preview_importable(
                force_job.id,
                preview_result={"verdict": "evidence_ready"},
            ))
        before = _claim_progress_facts(db, force_job.id)

        def no_systemd(**_kwargs: object) -> Never:
            raise ValueError("generated force worker has no systemd lease")

        def forbidden_preview(
            *_args: object,
            **_kwargs: object,
        ) -> ImportPreviewResult:
            raise AssertionError("lock miss reached force preview")

        def forbidden_import(
            *_args: object,
            **_kwargs: object,
        ) -> DispatchOutcome:
            raise AssertionError("lock miss reached force import")

        if lane == "preview":
            result = import_preview_worker.run_once(
                db,
                worker_id="generated-force-preview-miss",
                stage_db_factory=lambda _dsn: _GeneratedStageSession(
                    db,
                    acquire=False,
                ),
                execution_lease_factory=no_systemd,
                candidate_measurement_fn=forbidden_preview,
            )
        else:
            result = importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="generated-force-import-miss",
                stage_db_factory=lambda _dsn: _GeneratedStageSession(
                    db,
                    acquire=False,
                ),
                execution_lease_factory=no_systemd,
                execute_fn=forbidden_import,
            )
        self.assertIsNone(result)
        self.assertEqual(_claim_progress_facts(db, force_job.id), before)

        if request_changed:
            self.assertTrue(db.set_downloading(
                _REQUEST_ID,
                _active_state(_WITNESS_A).to_json(),
                expected_status="wanted",
            ))
        claimed = (
            claim_next_import_preview_job(db, worker_id="future-preview")
            if lane == "preview"
            else claim_next_import_job(db, worker_id="future-import")
        )
        self.assertEqual(claimed is not None, not request_changed)

    @given(witness=st.sampled_from(_WITNESSES))
    @example(witness=_WITNESS_B)
    def test_dead_preview_requeues_without_releasing_owner(
        self,
        witness: str,
    ) -> None:
        from scripts import import_preview_worker

        db, job_id = _new_owner_db(witness=witness)
        lease = _execution_lease("preview", job_id, 1)
        self.assertIsNotNone(claim_next_import_preview_job(db, worker_id="generated-preview",
        execution_lease=lease,))

        recovered = import_preview_worker.recover_running_preview_jobs(
            db,  # pyright: ignore[reportArgumentType]
            liveness_probe=_ChangedBootProbe(),
        )

        self.assertEqual([job.id for job in recovered], [job_id])
        request = db.request(_REQUEST_ID)
        job = db.get_import_job(job_id)
        assert job is not None
        self.assertTrue(_exact_processing_owner(request, job_id))
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.preview_status, IMPORT_JOB_PREVIEW_WAITING)
        self.assertIsNone(job.execution_invocation_id)

    @given(action=st.sampled_from(("delete", "search_stop")))
    @example(action="delete")
    def test_operator_invalidators_are_zero_mutation_while_owned(
        self,
        action: str,
    ) -> None:
        db, _job_id = _new_owner_db()
        before = _database_snapshot(db)
        if action == "delete":
            result = delete_pipeline_request(db, _REQUEST_ID)
        else:
            result = transitions.finalize_operator_request(
                db,
                _REQUEST_ID,
                transitions.RequestTransition.to_unsearchable(
                    from_status="processing",
                ),
            )
        self.assertIsInstance(result, transitions.TransitionConflict)
        assert isinstance(result, transitions.TransitionConflict)
        self.assertEqual(
            result.kind,
            transitions.TransitionConflictKind.processing_locked,
        )
        self.assertEqual(_database_snapshot(db), before)

    def test_known_bad_owner_predicate_without_pointer_is_detected(self) -> None:
        request = {
            "status": "processing",
            "active_automation_import_job_id": 11,
        }
        self.assertTrue(_mutant_owner_ignores_pointer(request, 12))
        self.assertFalse(_exact_processing_owner(request, 12))

    def test_known_bad_witness_blind_handoff_is_detected(self) -> None:
        self.assertTrue(_mutant_handoff_ignores_witness(
            status="downloading",
            _stored_witness=_WITNESS_A,
            _supplied_witness=_WITNESS_B,
            owner_job_id=None,
        ))
        self.assertFalse(_handoff_allowed(
            status="downloading",
            stored_witness=_WITNESS_A,
            supplied_witness=_WITNESS_B,
            owner_job_id=None,
        ))

    def test_known_bad_split_terminal_bundle_is_detected(self) -> None:
        before = _TerminalFacts(False, False, False, False)
        mutant = _TerminalFacts(
            request_released=True,
            audit_present=True,
            job_terminal=False,
            cleanup_consumed=False,
        )
        with self.assertRaisesRegex(AssertionError, "partial terminal bundle"):
            _assert_terminal_all_or_none(before, mutant)


    def test_known_bad_claim_before_lock_is_detected(self) -> None:
        cases: tuple[
            tuple[ClaimLane, _ClaimProgressFacts, _ClaimProgressFacts],
            ...,
        ] = (
            (
                "preview",
                _ClaimProgressFacts(
                    status="queued",
                    preview_status=IMPORT_JOB_PREVIEW_WAITING,
                    attempts=0,
                    preview_attempts=0,
                ),
                _ClaimProgressFacts(
                    status="queued",
                    preview_status=IMPORT_JOB_PREVIEW_RUNNING,
                    attempts=0,
                    preview_attempts=1,
                ),
            ),
            (
                "import",
                _ClaimProgressFacts(
                    status="queued",
                    preview_status=IMPORT_JOB_PREVIEW_EVIDENCE_READY,
                    attempts=0,
                    preview_attempts=1,
                ),
                _ClaimProgressFacts(
                    status="running",
                    preview_status=IMPORT_JOB_PREVIEW_EVIDENCE_READY,
                    attempts=1,
                    preview_attempts=1,
                ),
            ),
        )
        for lane, before, mutant in cases:
            with (
                self.subTest(lane=lane),
                self.assertRaisesRegex(
                    AssertionError,
                    f"{lane} lock miss created execution state",
                ),
            ):
                _assert_lock_miss_then_progress(
                    lane=lane,
                    before=before,
                    after_miss=mutant,
                    after_progress=mutant,
                )

    def test_known_bad_force_claim_after_owner_handoff_is_detected(self) -> None:
        before = _ClaimProgressFacts(
            status="queued",
            preview_status=IMPORT_JOB_PREVIEW_WAITING,
            attempts=0,
            preview_attempts=0,
        )
        mutant = replace(
            before,
            preview_status=IMPORT_JOB_PREVIEW_RUNNING,
            preview_attempts=1,
        )
        with self.assertRaisesRegex(
            AssertionError,
            "owned request admitted stale force",
        ):
            _assert_force_owner_fence(
                before,
                mutant,
                effect_count=1,
            )


TestProcessingLifecycleMachine = ProcessingLifecycleMachine.TestCase


if __name__ == "__main__":
    unittest.main()
