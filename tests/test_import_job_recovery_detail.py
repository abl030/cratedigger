"""Shared automation recovery-detail evidence contracts."""

from __future__ import annotations

import copy
import os
import sys
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

import msgspec

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.beets_db import (
    CurrentBeetsAmbiguous,
    CurrentBeetsItem,
    CurrentBeetsMissing,
    CurrentBeetsResolution,
    CurrentBeetsUnique,
)
from lib.import_execution import (
    CgroupObservation,
    CgroupState,
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    InvocationObservation,
    InvocationState,
    ProcessIdentity,
    ProcessObservation,
    ProcessState,
)
from lib.import_job_recovery_service import (
    AUTOMATION_COMPLETION_RESULT_KEY,
    AutomationCompletionReceipt,
    AutomationRecoveryBeets,
    automation_completion_result_patch,
    get_automation_recovery_detail,
)
from lib.import_queue import ImportJob
from lib.pipeline_db.cleanup_journal import ProcessingCleanupJournalRow
from lib.pipeline_db.rows import AlbumRequestRow
from lib.release_identity import ReleaseIdentity
from tests.helpers import (
    REQUEST_CASCADE_RESET_TABLES,
    delete_all_rows,
    make_request_row,
)

_RELEASE_ID = "75dbf62e-7dd2-4ddc-b57b-9bad1758b6b0"
_OBSERVED = datetime(2026, 7, 29, 1, 2, 3, tzinfo=UTC)
TEST_DSN = os.environ["TEST_DB_DSN"]


def _job(
    *,
    status: str = "running",
    launch: bool = False,
    result: dict[str, object] | None = None,
    completed_at: datetime | None = None,
    with_lease: bool = True,
    with_child: bool = True,
) -> ImportJob:
    row: dict[str, object] = {
        "id": 7,
        "job_type": "automation_import",
        "status": status,
        "request_id": 42,
        "payload": {},
        "result": result,
        "attempts": 1,
        "preview_status": "evidence_ready",
        "completed_at": completed_at,
        "beets_launch_authorized_at": _OBSERVED if launch else None,
        "beets_launch_release_id": _RELEASE_ID if launch else None,
        "beets_launch_source_path": "/processing/album" if launch else None,
        "beets_launch_request_status": "processing" if launch else None,
        "beets_launch_snapshot_fingerprint": "snapshot-a" if launch else None,
    }
    if with_lease:
        row.update({
            "execution_invocation_id": "invocation-a",
            "execution_host_boot_id": "boot-a",
            "execution_systemd_unit": "cratedigger-importer.service",
            "execution_worker_pid": 101,
            "execution_worker_start_ticks": 1001,
            "execution_beets_pid": 202 if with_child else None,
            "execution_beets_start_ticks": 2002 if with_child else None,
        })
    return ImportJob.from_row(row)


def _request() -> AlbumRequestRow:
    return msgspec.convert(
        make_request_row(
            id=42,
            status="processing",
            mb_release_id=_RELEASE_ID,
            active_automation_import_job_id=7,
            active_download_state={
                "filetype": "flac",
                "enqueued_at": "2026-07-29T00:00:00+00:00",
                "files": [],
                "processing_started_at": "2026-07-29T00:01:00+00:00",
                "current_path": "/processing/album",
            },
        ),
        type=AlbumRequestRow,
    )


class _DetailDB:
    def __init__(
        self,
        job: ImportJob,
        *,
        request: AlbumRequestRow | None = None,
        journal: ProcessingCleanupJournalRow | None = None,
        journal_error: Exception | None = None,
    ) -> None:
        self.job = job
        self.request = _request() if request is None else request
        self.journal = journal
        self.journal_error = journal_error

    def get_import_job(self, job_id: int) -> ImportJob | None:
        return self.job if job_id == self.job.id else None

    def get_request(self, request_id: int) -> AlbumRequestRow | None:
        return self.request if request_id == 42 else None

    def get_processing_cleanup_journal(
        self,
        *,
        request_id: int,
        job_id: int,
    ) -> ProcessingCleanupJournalRow | None:
        if self.journal_error is not None:
            raise self.journal_error
        if request_id != 42 or job_id != 7:
            return None
        return copy.deepcopy(self.journal)


class _Beets:
    def __init__(
        self,
        resolution: CurrentBeetsResolution | Exception,
    ) -> None:
        self.resolution = resolution

    def resolve_current_release(
        self,
        identity: ReleaseIdentity,
    ) -> CurrentBeetsResolution:
        if isinstance(self.resolution, Exception):
            raise self.resolution
        return self.resolution


class _Probe:
    def __init__(
        self,
        evidence: ExecutionLivenessEvidence | Exception,
    ) -> None:
        self.evidence = evidence

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        if isinstance(self.evidence, Exception):
            raise self.evidence
        return self.evidence


def _lease() -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="boot-a",
        invocation_id="invocation-a",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(101, 1001),
        beets=ProcessIdentity(202, 2002),
    )


def _process(
    identity: ProcessIdentity,
    state: ProcessState,
) -> ProcessObservation:
    return ProcessObservation(
        identity=identity,
        state=state,
        observed_start_ticks=(
            identity.start_ticks if state == "exact"
            else identity.start_ticks + 1 if state == "reused"
            else None
        ),
        cgroup_path=(
            "/system.slice/cratedigger-importer.service"
            if state == "exact" else None
        ),
        reason=f"process_{state}",
    )


def _same_boot_evidence(
    *,
    worker: ProcessState,
    child: ProcessState,
    invocation: InvocationState,
    cgroup: CgroupState,
) -> ExecutionLivenessEvidence:
    lease = _lease()
    return ExecutionLivenessEvidence(
        lease=lease,
        current_host_boot_id="boot-a",
        boot_error=None,
        worker=_process(lease.worker, worker),
        beets=_process(lease.beets, child) if lease.beets is not None else None,
        invocation=InvocationObservation(
            state=invocation,
            stored_invocation_id=lease.invocation_id,
            observed_invocation_id=(
                lease.invocation_id if invocation == "exact" else None
            ),
            control_group=(
                "/system.slice/cratedigger-importer.service"
                if invocation == "exact" else None
            ),
            reason=f"invocation_{invocation}",
            active_state="active" if invocation == "exact" else "inactive",
            sub_state="running" if invocation == "exact" else "dead",
        ),
        cgroup=CgroupObservation(
            state=cgroup,
            path=(
                "/system.slice/cratedigger-importer.service"
                if cgroup == "exact" else None
            ),
            member_pids=(101, 202) if cgroup == "exact" else (),
            reason=f"cgroup_{cgroup}",
        ),
    )


def _changed_boot_evidence() -> ExecutionLivenessEvidence:
    return ExecutionLivenessEvidence(
        lease=_lease(),
        current_host_boot_id="boot-b",
        boot_error=None,
        worker=None,
        beets=None,
        invocation=None,
        cgroup=None,
    )


def _missing_library() -> _Beets:
    identity = ReleaseIdentity.from_id(_RELEASE_ID)
    assert identity is not None
    return _Beets(CurrentBeetsMissing(identity))


def _detail(
    db: _DetailDB,
    *,
    beets: AutomationRecoveryBeets | None = None,
    probe: _Probe | None = None,
    observed_at: datetime = _OBSERVED,
):
    result = get_automation_recovery_detail(
        db,
        beets or _missing_library(),
        7,
        liveness_probe=probe,
        observed_at=observed_at,
    )
    assert result.detail is not None
    return result.detail


class TestAutomationRecoveryDetail(unittest.TestCase):

    def test_liveness_uses_shared_live_reused_dead_child_boot_and_unknown_transcripts(
        self,
    ) -> None:
        cases = {
            "live": (
                _Probe(_same_boot_evidence(
                    worker="exact",
                    child="exact",
                    invocation="exact",
                    cgroup="exact",
                )),
                "live",
                "exact_execution_present",
            ),
            "reused": (
                _Probe(_same_boot_evidence(
                    worker="reused",
                    child="reused",
                    invocation="ended",
                    cgroup="absent",
                )),
                "dead",
                "execution_ended",
            ),
            "dead_child": (
                _Probe(_same_boot_evidence(
                    worker="absent",
                    child="absent",
                    invocation="ended",
                    cgroup="absent",
                )),
                "dead",
                "execution_ended",
            ),
            "boot_changed": (
                _Probe(_changed_boot_evidence()),
                "dead",
                "boot_changed",
            ),
            "unknown": (
                _Probe(OSError("procfs unavailable")),
                "unknown",
                "probe_failed",
            ),
        }
        for name, (probe, status, reason) in cases.items():
            with self.subTest(name=name):
                detail = _detail(_DetailDB(_job()), probe=probe)
                self.assertEqual(detail.execution_liveness.status, status)
                self.assertEqual(detail.execution_liveness.reason, reason)
                self.assertEqual(
                    detail.execution_liveness.observed_at,
                    _OBSERVED.isoformat(),
                )

    def test_completion_is_absent_only_before_launch(self) -> None:
        queued = _detail(_DetailDB(_job(
            status="queued",
            with_lease=False,
            with_child=False,
        )))
        self.assertEqual(queued.completion.status, "absent")
        self.assertEqual(
            queued.completion.reason,
            "beets_launch_not_authorized",
        )

        launched = _detail(
            _DetailDB(_job(launch=True)),
            probe=_Probe(_changed_boot_evidence()),
        )
        self.assertEqual(launched.completion.status, "unavailable")
        self.assertEqual(
            launched.completion.reason,
            "automation_completion_receipt_missing",
        )

        malformed = _detail(
            _DetailDB(_job(
                launch=True,
                result={AUTOMATION_COMPLETION_RESULT_KEY: {"job_id": 7}},
            )),
            probe=_Probe(_changed_boot_evidence()),
        )
        self.assertEqual(malformed.completion.status, "unavailable")
        self.assertEqual(
            malformed.completion.reason,
            "automation_completion_receipt_invalid",
        )
        null_receipt = _detail(_DetailDB(_job(
            status="queued",
            result={AUTOMATION_COMPLETION_RESULT_KEY: None},
            with_lease=False,
            with_child=False,
        )))
        self.assertEqual(null_receipt.completion.status, "unavailable")
        self.assertEqual(
            null_receipt.completion.reason,
            "automation_completion_receipt_invalid",
        )

    def test_typed_completion_receipt_is_captured_only_for_exact_owner(
        self,
    ) -> None:
        receipt = AutomationCompletionReceipt(
            job_id=7,
            request_id=42,
            release_id=_RELEASE_ID,
            canonical_path="/processing/album",
            returncode=0,
            captured_at="2026-07-29T01:02:04+00:00",
        )
        patch = automation_completion_result_patch(receipt)
        persisted_receipt = msgspec.convert(
            patch[AUTOMATION_COMPLETION_RESULT_KEY],
            type=dict[str, object],
        )
        self.assertEqual(
            persisted_receipt["job_id"],
            7,
        )
        detail = _detail(
            _DetailDB(_job(launch=True, result=patch)),
            probe=_Probe(_changed_boot_evidence()),
        )
        self.assertEqual(detail.completion.status, "captured")
        self.assertEqual(detail.completion.receipt, receipt)

        wrong_owner = _request()
        wrong_owner["active_automation_import_job_id"] = 99
        unavailable = _detail(
            _DetailDB(
                _job(launch=True, result=patch),
                request=wrong_owner,
            ),
            probe=_Probe(_changed_boot_evidence()),
        )
        self.assertEqual(unavailable.completion.status, "unavailable")

    def test_exact_library_states_do_not_collapse_probe_failure_to_missing(
        self,
    ) -> None:
        identity = ReleaseIdentity.from_id(_RELEASE_ID)
        assert identity is not None
        states: tuple[
            tuple[CurrentBeetsResolution | Exception, str],
            ...,
        ] = (
            (
                CurrentBeetsUnique(
                    identity=identity,
                    album_id=11,
                    album_path="/library/album",
                    items=(CurrentBeetsItem(id=1, path="/library/a.flac"),),
                    selectors=("mb_albumid:" + _RELEASE_ID,),
                ),
                "unique",
            ),
            (CurrentBeetsMissing(identity), "missing"),
            (
                CurrentBeetsAmbiguous(
                    identity=identity,
                    album_ids=(11, 12),
                    reason="multiple_matches",
                ),
                "ambiguous",
            ),
            (OSError("locked"), "unavailable"),
        )
        for resolution, expected in states:
            with self.subTest(expected=expected):
                detail = _detail(
                    _DetailDB(_job(
                        status="queued",
                        with_lease=False,
                        with_child=False,
                    )),
                    beets=_Beets(resolution),
                )
                self.assertEqual(detail.exact_library.status, expected)

    def test_cleanup_progress_is_visible_without_operator_action_metadata(
        self,
    ) -> None:
        journal = ProcessingCleanupJournalRow(
            job_id=7,
            request_id=42,
            revision=3,
            action="remove_source",
            source_path="/processing/album",
            source_manifest=[],
            source_manifest_hash="sha256:a",
            destination_path=None,
            destination_manifest=None,
            destination_manifest_hash=None,
            selected_destination_path=None,
            step_progress={"removed": 1},
            completed_receipt=None,
            created_at=_OBSERVED,
            updated_at=_OBSERVED,
            completed_at=None,
        )
        probe = _Probe(_same_boot_evidence(
            worker="absent",
            child="absent",
            invocation="ended",
            cgroup="absent",
        ))
        first = _detail(
            _DetailDB(_job(), journal=journal),
            probe=probe,
        )
        later = _detail(
            _DetailDB(_job(), journal=journal),
            probe=probe,
            observed_at=datetime(2026, 7, 30, tzinfo=UTC),
        )
        self.assertEqual(first.cleanup_journal.revision, 3)
        self.assertEqual(first.cleanup_journal.step_progress, {"removed": 1})
        self.assertEqual(later.cleanup_journal.revision, 3)

        changed = copy.deepcopy(journal)
        changed["revision"] = 4
        changed["step_progress"] = {"removed": 2}
        after_checkpoint = _detail(
            _DetailDB(_job(), journal=changed),
            probe=probe,
        )
        self.assertEqual(after_checkpoint.cleanup_journal.revision, 4)
        self.assertEqual(
            after_checkpoint.cleanup_journal.step_progress,
            {"removed": 2},
        )

    def test_missing_journal_is_visible_but_does_not_claim_cleanup_is_done(
        self,
    ) -> None:
        detail = _detail(
            _DetailDB(_job()),
            probe=_Probe(_same_boot_evidence(
                worker="absent",
                child="absent",
                invocation="ended",
                cgroup="absent",
            )),
        )
        self.assertEqual(detail.cleanup_journal.status, "missing")

        failed_probe = _detail(
            _DetailDB(_job(), journal_error=OSError("database unavailable")),
            probe=_Probe(_same_boot_evidence(
                worker="absent",
                child="absent",
                invocation="ended",
                cgroup="absent",
            )),
        )
        self.assertEqual(failed_probe.cleanup_journal.status, "unavailable")
        self.assertEqual(
            failed_probe.cleanup_journal.reason,
            "cleanup_journal_probe_failed:OSError",
        )


class TestAutomationRecoveryDetailPostgres(unittest.TestCase):
    def setUp(self) -> None:
        from lib.pipeline_db import PipelineDB

        self.db = PipelineDB(TEST_DSN)
        delete_all_rows(self.db, REQUEST_CASCADE_RESET_TABLES)

    def tearDown(self) -> None:
        self.db.close()

    def test_real_and_fake_reads_share_the_same_recovery_states(self) -> None:
        from tests.dispatch_helpers import handoff_automation_owner
        from tests.fakes import FakePipelineDB

        request_id = self.db.add_request(
            "Recovery Artist",
            "Recovery Album",
            "request",
            mb_release_id=_RELEASE_ID,
        )
        real_job = handoff_automation_owner(
            self.db,
            request_id,
            canonical_path="/processing/parity",
        )
        real_result = get_automation_recovery_detail(
            self.db,
            None,
            real_job.id,
            observed_at=_OBSERVED,
        )
        assert real_result.detail is not None

        fake = FakePipelineDB()
        fake.seed_request(make_request_row(
            id=request_id,
            status="wanted",
            mb_release_id=_RELEASE_ID,
        ))
        fake_job = handoff_automation_owner(
            fake,
            request_id,
            canonical_path="/processing/parity",
        )
        with patch.object(
            fake,
            "get_processing_cleanup_journal",
            lambda *, request_id, job_id: None,
            create=True,
        ):
            fake_result = get_automation_recovery_detail(
                fake,
                None,
                fake_job.id,
                observed_at=_OBSERVED,
            )
        assert fake_result.detail is not None

        for detail in (real_result.detail, fake_result.detail):
            self.assertEqual(detail.owner_stage.job_status, "queued")
            self.assertTrue(detail.owner_stage.exact_active_owner)
            self.assertEqual(detail.canonical_path, "/processing/parity")
            self.assertEqual(detail.execution_liveness.status, "dead")
            self.assertEqual(detail.completion.status, "absent")
            self.assertEqual(detail.exact_library.status, "unavailable")
            self.assertEqual(detail.cleanup_journal.status, "missing")


if __name__ == "__main__":
    unittest.main()
