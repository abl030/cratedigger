"""Generated end-to-end processing-owner lifecycle proof (#898).

The state machine drives the production-parity database commands and services
for the whole bounded lifecycle:

``wanted -> downloading -> processing -> {wanted, imported}``

It deliberately interleaves stale A/B download witnesses, exact and wrong job
IDs, preview/import execution death, recovery retry/close, and operator
invalidators.  A small independent oracle checks the ownership predicate,
handoff witness, terminal all-or-none shape, and recovery retarget.

Profiles, promotion policy, and fault-injection qualification:
``docs/generated-testing.md``.
"""

from __future__ import annotations

import copy
import os
import unittest
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

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
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ExecutionLivenessProbe,
    ProcessIdentity,
)
from lib.import_job_recovery_service import (
    AutomationRecoveryMutationDB,
    apply_import_job_recovery,
    get_automation_recovery_detail,
)
from lib.import_queue import (
    IMPORT_JOB_ACTIVE_STATUSES,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_RUNNING,
    IMPORT_JOB_PREVIEW_WAITING,
    IMPORT_JOB_RECOVERY_REQUIRED,
)
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.pipeline_delete_service import delete_pipeline_request
from lib.processing_cleanup import (
    cleanup_manifest_hash,
    execute_processing_cleanup,
)
from lib.quality import ActiveDownloadState
from lib.terminal_outcomes import automation_recovery_close_outcome
from tests.fakes import FakePipelineDB
from tests.helpers import make_album_quality_evidence, make_request_row

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
    "recovery_required",
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
    cleanup_journal_present: bool = False


@dataclass(frozen=True)
class _TerminalFacts:
    request_released: bool
    audit_present: bool
    job_terminal: bool
    cleanup_consumed: bool


class _InjectedLifecycleFailure(RuntimeError):
    pass


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
    request: dict[str, Any],
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


def _assert_retry_retarget(
    *,
    old_job_id: int,
    old_status: str,
    new_job_id: int,
    new_status: str,
    request_owner_job_id: int | None,
    old_journal_present: bool,
    new_journal_revision: int | None,
    expected_new_journal_revision: int | None,
) -> None:
    """The fresh job, request pointer, and journal move as one unit."""
    if old_status != "failed":
        raise AssertionError(
            f"retry left old job {old_job_id} active as {old_status!r}"
        )
    expected_status = (
        "recovery_required"
        if expected_new_journal_revision is not None
        else "queued"
    )
    if new_job_id == old_job_id or new_status != expected_status:
        raise AssertionError(
            f"retry did not create a fresh {expected_status} job: "
            f"{old_job_id} -> {new_job_id} ({new_status!r})"
        )
    if request_owner_job_id != new_job_id:
        raise AssertionError(
            f"request still points at {request_owner_job_id!r}, "
            f"want {new_job_id}"
        )
    if old_journal_present:
        raise AssertionError("retry retained the cleanup journal on the old job")
    if new_journal_revision != expected_new_journal_revision:
        raise AssertionError(
            "retry journal retarget mismatch: "
            f"{new_journal_revision!r} != {expected_new_journal_revision!r}"
        )


def _mutant_owner_ignores_pointer(
    request: dict[str, Any],
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


def _database_snapshot(db: FakePipelineDB) -> object:
    """All state that an owner-conflicted command is forbidden to mutate."""
    return copy.deepcopy((
        db._requests,
        db._import_jobs,
        db._processing_cleanup_journals,
        db.download_logs,
        db.status_history,
    ))


def _terminal_facts(
    db: FakePipelineDB,
    *,
    request_id: int,
    job_id: int,
) -> _TerminalFacts:
    request = db.request(request_id)
    job = db.get_import_job(job_id)
    assert job is not None
    return _TerminalFacts(
        request_released=(
            request["status"] == "wanted"
            and request["active_automation_import_job_id"] is None
        ),
        audit_present=bool(db.get_download_history(request_id)),
        job_terminal=job.status == "failed",
        cleanup_consumed=(
            db.get_processing_cleanup_journal(
                request_id=request_id,
                job_id=job_id,
            )
            is None
        ),
    )


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


def _launched_owner_db(
    *,
    witness: str = _WITNESS_A,
) -> tuple[FakePipelineDB, int, ExecutionLeaseSnapshot]:
    db, job_id = _new_owner_db(witness=witness)
    preview_lease = _execution_lease("preview", job_id, 1)
    claimed_preview = db.claim_next_import_preview_job(
        worker_id="generated-preview",
        execution_lease=preview_lease,
    )
    assert claimed_preview is not None and claimed_preview.id == job_id
    _seed_candidate(
        db,
        job_id=job_id,
        execution_lease=preview_lease,
    )
    assert db.mark_import_job_preview_importable(
        job_id,
        preview_result={"ready": True},
        expected_execution_lease=preview_lease,
    ) is not None
    import_lease = _execution_lease("import", job_id, 2)
    claimed_import = db.claim_next_import_job(
        worker_id="generated-import",
        execution_lease=import_lease,
    )
    assert claimed_import is not None and claimed_import.id == job_id
    assert db.authorize_import_job_launch(
        job_id,
        request_id=_REQUEST_ID,
        release_id=_RELEASE_ID,
        source_path=_CANONICAL_PATH,
        expected_execution_lease=import_lease,
    ) is not None
    return db, job_id, import_lease


def _recover_launched_owner(
    db: FakePipelineDB,
    *,
    job_id: int,
) -> None:
    from scripts import importer

    recovered = importer.recover_abandoned_running_jobs(
        cast(Any, db),
        liveness_probe=cast(ExecutionLivenessProbe, _ChangedBootProbe()),
    )
    if [job.id for job in recovered] != [job_id]:
        raise AssertionError(
            f"dead exact import execution was not recovered: {recovered!r}"
        )
    current = db.get_import_job(job_id)
    assert current is not None
    if current.status != IMPORT_JOB_RECOVERY_REQUIRED:
        raise AssertionError(
            f"dead launched owner became {current.status!r}, "
            "want recovery_required"
        )


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
        claimed = self.db.claim_next_import_preview_job(
            worker_id="generated-preview",
            execution_lease=lease,
        )
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
            cast(Any, self.db),
            liveness_probe=cast(
                ExecutionLivenessProbe,
                _ChangedBootProbe(),
            ),
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
        claimed = self.db.claim_next_import_job(
            worker_id="generated-import",
            execution_lease=lease,
        )
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
        _recover_launched_owner(
            self.db,
            job_id=self.oracle.owner_job_id,
        )
        self.oracle.stage = "recovery_required"

    @precondition(
        lambda self: self.oracle.stage == "recovery_required"
        and self.oracle.owner_job_id is not None
    )
    @rule(with_journal=st.booleans())
    def retry_recovery(self, with_journal: bool) -> None:
        assert self.oracle.owner_job_id is not None
        old_job_id = self.oracle.owner_job_id
        journal = self.db.get_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=old_job_id,
        )
        if journal is None and with_journal:
            journal = self.db.create_processing_cleanup_journal(
                request_id=_REQUEST_ID,
                job_id=old_job_id,
                intent=CleanupJournalIntent(
                    action="no_op",
                    source_path=_CANONICAL_PATH,
                    source_manifest=(),
                    source_manifest_hash=cleanup_manifest_hash(()),
                ),
            )
        old_revision = None if journal is None else journal["revision"]
        probe = cast(ExecutionLivenessProbe, _ChangedBootProbe())
        detail = get_automation_recovery_detail(
            cast(AutomationRecoveryMutationDB, self.db),
            None,
            old_job_id,
            liveness_probe=probe,
        )
        assert detail.detail is not None
        result = apply_import_job_recovery(
            cast(AutomationRecoveryMutationDB, self.db),
            None,
            old_job_id,
            action="retry",
            reason="generated exact-dead retry",
            evidence_revision=detail.detail.evidence_revision,
            liveness_probe=probe,
        )
        expected_outcome = (
            "retry_recovery_required"
            if journal is not None
            else "retry_queued"
        )
        self.assert_or_raise(result.outcome == expected_outcome)
        assert result.retry_job is not None
        old = self.db.get_import_job(old_job_id)
        new = self.db.get_import_job(result.retry_job.id)
        assert old is not None and new is not None
        moved = self.db.get_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=result.retry_job.id,
        )
        _assert_retry_retarget(
            old_job_id=old_job_id,
            old_status=old.status,
            new_job_id=new.id,
            new_status=new.status,
            request_owner_job_id=self.db.request(_REQUEST_ID)[
                "active_automation_import_job_id"
            ],
            old_journal_present=(
                self.db.get_processing_cleanup_journal(
                    request_id=_REQUEST_ID,
                    job_id=old_job_id,
                )
                is not None
            ),
            new_journal_revision=(
                None if moved is None else moved["revision"]
            ),
            expected_new_journal_revision=(
                None if old_revision is None else old_revision + 1
            ),
        )
        if journal is not None:
            self.assert_or_raise(self.db.claim_next_import_preview_job(
                worker_id="generated-must-not-replay-preview",
                execution_lease=_execution_lease(
                    "preview",
                    new.id,
                    self.generation + 1,
                ),
            ) is None)
            self.assert_or_raise(self.db.claim_next_import_job(
                worker_id="generated-must-not-replay-import",
                execution_lease=_execution_lease(
                    "import",
                    new.id,
                    self.generation + 1,
                ),
            ) is None)
        self.oracle.owner_job_id = new.id
        self.oracle.stage = (
            "recovery_required"
            if journal is not None
            else "preview_ready"
        )
        self.oracle.preview_lease = None
        self.oracle.import_lease = None
        self.oracle.launched = False
        self.oracle.cleanup_journal_present = journal is not None

    @precondition(
        lambda self: self.oracle.stage == "recovery_required"
        and self.oracle.owner_job_id is not None
        and not self.oracle.cleanup_journal_present
    )
    @rule(result_status=st.sampled_from(("wanted", "imported")))
    def close_recovery(self, result_status: str) -> None:
        assert self.oracle.owner_job_id is not None
        job_id = self.oracle.owner_job_id
        probe = cast(ExecutionLivenessProbe, _ChangedBootProbe())
        detail = get_automation_recovery_detail(
            cast(AutomationRecoveryMutationDB, self.db),
            None,
            job_id,
            liveness_probe=probe,
        )
        assert detail.detail is not None
        result = apply_import_job_recovery(
            cast(AutomationRecoveryMutationDB, self.db),
            None,
            job_id,
            action="close",
            reason="generated explicit recovery close",
            evidence_revision=detail.detail.evidence_revision,
            result_status=cast(Literal["wanted", "imported"], result_status),
            liveness_probe=probe,
        )
        self.assert_or_raise(result.outcome == "closed")
        self.oracle.status = cast(LifecycleStatus, result_status)
        self.oracle.stage = "none"
        self.oracle.witness = None
        self.oracle.owner_job_id = None
        self.oracle.preview_lease = None
        self.oracle.import_lease = None
        self.oracle.launched = False
        self.oracle.cleanup_journal_present = False

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
        journal = self.db.get_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=job.id,
        )
        self.assert_or_raise(
            (journal is not None) == self.oracle.cleanup_journal_present
        )
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
            "recovery_required": (
                IMPORT_JOB_RECOVERY_REQUIRED,
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

    @given(witness=st.sampled_from(_WITNESSES))
    @example(witness=_WITNESS_B)
    def test_dead_preview_requeues_without_releasing_owner(
        self,
        witness: str,
    ) -> None:
        from scripts import import_preview_worker

        db, job_id = _new_owner_db(witness=witness)
        lease = _execution_lease("preview", job_id, 1)
        self.assertIsNotNone(db.claim_next_import_preview_job(
            worker_id="generated-preview",
            execution_lease=lease,
        ))

        recovered = import_preview_worker.recover_running_preview_jobs(
            cast(Any, db),
            liveness_probe=cast(
                ExecutionLivenessProbe,
                _ChangedBootProbe(),
            ),
        )

        self.assertEqual([job.id for job in recovered], [job_id])
        request = db.request(_REQUEST_ID)
        job = db.get_import_job(job_id)
        assert job is not None
        self.assertTrue(_exact_processing_owner(request, job_id))
        self.assertEqual(job.status, "queued")
        self.assertEqual(job.preview_status, IMPORT_JOB_PREVIEW_WAITING)
        self.assertIsNone(job.execution_invocation_id)

    @given(with_journal=st.booleans())
    @example(with_journal=True)
    def test_dead_import_retry_retargets_every_authority(
        self,
        with_journal: bool,
    ) -> None:
        db, old_job_id, _lease = _launched_owner_db()
        _recover_launched_owner(db, job_id=old_job_id)
        old_revision: int | None = None
        if with_journal:
            journal = db.create_processing_cleanup_journal(
                request_id=_REQUEST_ID,
                job_id=old_job_id,
                intent=CleanupJournalIntent(
                    action="no_op",
                    source_path=_CANONICAL_PATH,
                    source_manifest=(),
                    source_manifest_hash=cleanup_manifest_hash(()),
                ),
            )
            old_revision = journal["revision"]
        probe = cast(ExecutionLivenessProbe, _ChangedBootProbe())
        observed = get_automation_recovery_detail(
            cast(AutomationRecoveryMutationDB, db),
            None,
            old_job_id,
            liveness_probe=probe,
        )
        assert observed.detail is not None

        result = apply_import_job_recovery(
            cast(AutomationRecoveryMutationDB, db),
            None,
            old_job_id,
            action="retry",
            reason="generated retry retarget",
            evidence_revision=observed.detail.evidence_revision,
            liveness_probe=probe,
        )

        self.assertEqual(
            result.outcome,
            (
                "retry_recovery_required"
                if with_journal
                else "retry_queued"
            ),
        )
        assert result.retry_job is not None
        old = db.get_import_job(old_job_id)
        new = db.get_import_job(result.retry_job.id)
        assert old is not None and new is not None
        moved = db.get_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=new.id,
        )
        _assert_retry_retarget(
            old_job_id=old_job_id,
            old_status=old.status,
            new_job_id=new.id,
            new_status=new.status,
            request_owner_job_id=db.request(_REQUEST_ID)[
                "active_automation_import_job_id"
            ],
            old_journal_present=(
                db.get_processing_cleanup_journal(
                    request_id=_REQUEST_ID,
                    job_id=old_job_id,
                )
                is not None
            ),
            new_journal_revision=(
                None if moved is None else moved["revision"]
            ),
            expected_new_journal_revision=(
                None if old_revision is None else old_revision + 1
            ),
        )
        if with_journal:
            self.assertIsNone(db.claim_next_import_preview_job(
                worker_id="generated-must-not-replay-preview",
                execution_lease=_execution_lease("preview", new.id, 2),
            ))
            self.assertIsNone(db.claim_next_import_job(
                worker_id="generated-must-not-replay-import",
                execution_lease=_execution_lease("import", new.id, 2),
            ))

    @given(result_status=st.sampled_from(("wanted", "imported")))
    @example(result_status="imported")
    def test_recovery_close_is_explicit_and_consumes_owner(
        self,
        result_status: str,
    ) -> None:
        db, job_id, _lease = _launched_owner_db()
        _recover_launched_owner(db, job_id=job_id)
        probe = cast(ExecutionLivenessProbe, _ChangedBootProbe())
        observed = get_automation_recovery_detail(
            cast(AutomationRecoveryMutationDB, db),
            None,
            job_id,
            liveness_probe=probe,
        )
        assert observed.detail is not None

        result = apply_import_job_recovery(
            cast(AutomationRecoveryMutationDB, db),
            None,
            job_id,
            action="close",
            reason=f"generated explicit {result_status}",
            evidence_revision=observed.detail.evidence_revision,
            result_status=cast(Literal["wanted", "imported"], result_status),
            liveness_probe=probe,
        )

        self.assertEqual(result.outcome, "closed")
        request = db.request(_REQUEST_ID)
        closed = db.get_import_job(job_id)
        assert closed is not None
        self.assertEqual(request["status"], result_status)
        self.assertIsNone(request["active_automation_import_job_id"])
        self.assertIsNone(request["active_download_state"])
        self.assertEqual(closed.status, "failed")
        self.assertIsNone(db.get_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=job_id,
        ))

    @given(fail_after=st.integers(min_value=0, max_value=4))
    @example(fail_after=3)
    def test_terminal_bundle_is_all_or_none_at_every_write_boundary(
        self,
        fail_after: int,
    ) -> None:
        db, job_id = _new_owner_db()
        reason = "generated terminal bundle"
        evidence_revision = "generated-terminal-revision"
        journal = db.create_processing_cleanup_journal(
            request_id=_REQUEST_ID,
            job_id=job_id,
            intent=CleanupJournalIntent(
                action="no_op",
                source_path=_CANONICAL_PATH,
                source_manifest=(),
                source_manifest_hash=cleanup_manifest_hash(()),
                declared_result_status="wanted",
                declared_reason=reason,
                evidence_revision=evidence_revision,
            ),
        )
        completed = execute_processing_cleanup(
            db,
            journal,
            owner_checkpoint=lambda: self.assertTrue(
                _exact_processing_owner(db.request(_REQUEST_ID), job_id)
            ),
        )
        receipt = completed["completed_receipt"]
        assert receipt is not None
        command = automation_recovery_close_outcome(
            request_id=_REQUEST_ID,
            import_job_id=job_id,
            result_status="wanted",
            reason=reason,
            evidence_revision=evidence_revision,
            expected_job_status="queued",
            expected_preview_status=IMPORT_JOB_PREVIEW_WAITING,
            expected_execution_lease=None,
            cleanup_receipt=receipt,
            completion_receipt=None,
        )
        before = _terminal_facts(
            db,
            request_id=_REQUEST_ID,
            job_id=job_id,
        )

        if fail_after:
            def fail_boundary(index: int, label: str) -> None:
                if index == fail_after:
                    raise _InjectedLifecycleFailure(label)

            db._terminal_outcome_write_boundary = fail_boundary
            with self.assertRaises(_InjectedLifecycleFailure):
                db.persist_import_terminal_outcome(command)
        else:
            db.persist_import_terminal_outcome(command)

        after = _terminal_facts(
            db,
            request_id=_REQUEST_ID,
            job_id=job_id,
        )
        _assert_terminal_all_or_none(before, after)
        if fail_after:
            self.assertEqual(after, before)
        else:
            self.assertEqual(after, _TerminalFacts(True, True, True, True))

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

    def test_known_bad_retry_without_retarget_is_detected(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "request still points",
        ):
            _assert_retry_retarget(
                old_job_id=11,
                old_status="failed",
                new_job_id=12,
                new_status="queued",
                request_owner_job_id=11,
                old_journal_present=False,
                new_journal_revision=2,
                expected_new_journal_revision=2,
            )


TestProcessingLifecycleMachine = ProcessingLifecycleMachine.TestCase


if __name__ == "__main__":
    unittest.main()
