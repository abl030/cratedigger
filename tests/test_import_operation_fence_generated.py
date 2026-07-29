"""Generated lifecycle proof for the Beets operation fence (#703)."""

from __future__ import annotations

import os
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from lib.config import CratediggerConfig
from lib.dispatch import dispatch_import_core
from lib.dispatch.types import EvidenceImportGate, ImportOneRun
from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessEvidence,
    ExecutionCancelled,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    youtube_import_payload,
)
from lib.quality import DownloadInfo
from scripts.importer import _execution_lease_from_job, process_claimed_job
from tests.beets_world import BeetsWorld
from tests.fakes import FakePipelineDB
from tests.helpers import (
    handoff_automation_owner,
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    pinned_dispatch_authority,
)


@dataclass(frozen=True)
class OperationWorld:
    job_type: str
    authority: str
    terminal_acknowledged: bool


def _execution_lease(job_id: int, *, lane: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-operation-fence-boot",
        invocation_id=f"generated-operation-fence-{lane}-{job_id}",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(
            pid=70_300 + job_id,
            start_ticks=703_000 + job_id,
        ),
    )


def _dead_execution(
    lease: ExecutionLeaseSnapshot,
) -> ExecutionLivenessDecision:
    return ExecutionLivenessDecision(
        status="dead",
        reason="generated prior worker exited",
        evidence=ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="generated-operation-fence-next-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        ),
    )


def assert_operation_fence(
    *,
    authorized: bool,
    terminal_acknowledged: bool,
    final_status: str,
    beets_invocations: list[int],
    replay_claimed: bool,
) -> None:
    """Every ambiguous authorized operation stops before an automatic replay."""
    if len(beets_invocations) > 1:
        raise AssertionError("one operation identity reached Beets more than once")
    if authorized and not terminal_acknowledged:
        if final_status != IMPORT_JOB_RECOVERY_REQUIRED:
            raise AssertionError("ambiguous Beets operation did not stop for recovery")
        if replay_claimed:
            raise AssertionError("ambiguous Beets operation became claimable")
    if not authorized and beets_invocations:
        raise AssertionError("Beets ran without exact current authority")


def _exercise_world(
    world: OperationWorld,
    *,
    beets: BeetsWorld,
) -> tuple[bool, str, list[int], bool]:
    db = FakePipelineDB()
    request_id = 703
    release_id = "release-703"
    source_path = "/tmp/fence-source"
    active_state = (
        {
            "current_path": source_path,
            "filetype": "flac",
            "enqueued_at": "2026-07-29T00:00:00+00:00",
            "files": [],
        }
        if world.job_type == IMPORT_JOB_AUTOMATION
        else None
    )
    db.seed_request(make_request_row(
        id=request_id,
        mb_release_id=release_id,
        status="wanted",
    ))
    if world.job_type == IMPORT_JOB_AUTOMATION:
        payload: dict[str, object] = {}
    elif world.job_type == IMPORT_JOB_FORCE:
        payload = {"download_log_id": request_id, "failed_path": source_path}
    else:
        payload = youtube_import_payload(
            staged_path=source_path,
            request_id=request_id,
            browse_id="MPREb_fence",
            download_log_id=request_id,
        )
    if world.job_type == IMPORT_JOB_AUTOMATION:
        assert active_state is not None
        job = handoff_automation_owner(
            db,
            request_id,
            state=active_state,
            canonical_path=source_path,
            message="generated operation-fence owner",
        )
    else:
        job = db.enqueue_import_job(
            world.job_type,
            request_id=request_id,
            dedupe_key=f"{world.job_type}:generated:{request_id}",
            payload=payload,
        )
    evidence = make_album_quality_evidence(
        mb_release_id=release_id,
        source_path=source_path,
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    importer_lease: ExecutionLeaseSnapshot | None = None
    if world.job_type == IMPORT_JOB_AUTOMATION:
        preview_lease = _execution_lease(job.id, lane="preview")
        preview_claim = db.claim_next_import_preview_job(
            worker_id="generated-preview-worker",
            execution_lease=preview_lease,
        )
        assert preview_claim is not None and preview_claim.id == job.id
        assert db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
            expected_execution_lease=preview_lease,
        )
        assert db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
            expected_execution_lease=preview_lease,
        ) is not None
        importer_lease = _execution_lease(job.id, lane="importer")
    else:
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"ready": True},
        )
    claimed = db.claim_next_import_job(
        worker_id="generated-worker",
        execution_lease=importer_lease,
    )
    assert claimed is not None
    if world.job_type == IMPORT_JOB_AUTOMATION:
        importer_lease = _execution_lease_from_job(claimed)
        assert importer_lease is not None

    launch_release = release_id
    launch_source = source_path
    if world.authority == "release_changed":
        db.request(request_id)["mb_release_id"] = "replacement-release"
    elif world.authority == "status_changed":
        db.request(request_id)["status"] = "imported"
    elif world.authority == "source_changed":
        launch_source = "/tmp/stale-source"

    beets_invocations: list[int] = []

    class RecorderStop(RuntimeError):
        pass

    def record_beets_invocation(**_kwargs: object) -> ImportOneRun:
        beets_invocations.append(claimed.id)
        if world.terminal_acknowledged:
            return ImportOneRun(
                command=("import_one",),
                returncode=0,
                stdout="",
                stderr="",
                import_result=make_import_result(
                    decision="import",
                    new_min_bitrate=245,
                ),
            )
        raise RecorderStop("stop immediately after the real Beets seam")

    def execute(
        db_arg,
        job_arg,
        *,
        ctx=None,
        execution_lease=None,
        cancellation_token=None,
        owner_session_identity=None,
    ):
        del ctx
        return dispatch_import_core(
            path=launch_source,
            mb_release_id=launch_release,
            request_id=request_id,
            label="Generated fence world",
            force=world.job_type == IMPORT_JOB_FORCE,
            beets_harness_path="/nix/store/fake/harness/run_beets_harness.sh",
            db=db_arg,
            dl_info=DownloadInfo(username="generated-peer"),
            distance=0.05,
            scenario=(
                "force_import"
                if world.job_type == IMPORT_JOB_FORCE
                else "strong_match"
            ),
            cfg=CratediggerConfig(
                beets_harness_path=(
                    "/nix/store/fake/harness/run_beets_harness.sh"
                ),
                pipeline_db_enabled=True,
            ),
            candidate_import_job_id=job_arg.id,
            quality_gate_fn=noop_quality_gate,
            evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(
                candidate=persisted,
            ),
            run_import_fn=record_beets_invocation,
            beets_library_db_path=str(beets.library_db),
            beets_library_root=str(beets.library_root),
            execution_lease=execution_lease,
            cancellation_token=cancellation_token,
            owner_session_identity=owner_session_identity,
        )

    if world.authority != "not_executed":
        with pinned_dispatch_authority(
            db,
            importer_lease,
        ) as (cancellation_token, owner_session_identity):
            try:
                process_claimed_job(
                    db,  # type: ignore[arg-type]
                    claimed,
                    execute_fn=execute,
                    execution_lease=importer_lease,
                    cancellation_token=cancellation_token,
                    owner_session_identity=owner_session_identity,
                )
            except ExecutionCancelled:
                if world.authority == "current":
                    raise
    launched_job = db.get_import_job(claimed.id)
    assert launched_job is not None
    authorized = launched_job.beets_launch_authorized_at is not None

    if world.job_type == IMPORT_JOB_AUTOMATION:
        assert importer_lease is not None
        db.recover_automation_import_job(
            claimed.id,
            expected_execution_lease=importer_lease,
            decision=_dead_execution(importer_lease),
            requeue_message="proven unstarted",
            recovery_message="operator recovery required",
        )
    else:
        db.recover_running_import_jobs(
            requeue_message="proven unstarted",
            recovery_message="operator recovery required",
        )
    if world.job_type == IMPORT_JOB_AUTOMATION:
        replay = db.claim_next_import_job(
            worker_id="automatic-import-replay",
            execution_lease=_execution_lease(claimed.id, lane="importer-replay"),
        )
    else:
        replay = db.claim_next_import_job(worker_id="automatic-replay")
    replay_claimed = replay is not None

    final = db.get_import_job(claimed.id)
    assert final is not None
    return authorized, final.status, beets_invocations, replay_claimed


class TestGeneratedImportOperationFence(unittest.TestCase):
    def setUp(self) -> None:
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        self.beets = BeetsWorld(repo_root)
        self.addCleanup(self.beets.close)
        self.runtime = patch.dict(os.environ, {
            "CRATEDIGGER_RUNTIME_CONFIG": str(
                self.beets.poisoned_runtime_config()
            ),
            "BEETS_DB": str(self.beets.root / "poisoned-library.db"),
        })
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    def test_stale_authority_never_launches_beets(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            for stale_dimension in (
                "release_changed",
                "status_changed",
                "source_changed",
            ):
                with self.subTest(
                    job_type=job_type,
                    stale_dimension=stale_dimension,
                ):
                    world = OperationWorld(job_type, stale_dimension, False)
                    authorized, status, invocations, replay_claimed = _exercise_world(
                        world, beets=self.beets
                    )
                    self.assertFalse(authorized)
                    self.assertEqual(invocations, [])
                    assert_operation_fence(
                        authorized=authorized,
                        terminal_acknowledged=False,
                        final_status=status,
                        beets_invocations=invocations,
                        replay_claimed=replay_claimed,
                    )

    def test_definitely_not_started_recovery_may_retry(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, _status, invocations, replay_claimed = _exercise_world(
                    OperationWorld(job_type, "not_executed", False),
                    beets=self.beets,
                )
                self.assertFalse(authorized)
                self.assertTrue(replay_claimed)
                self.assertEqual(invocations, [])

    def test_may_have_started_recovery_never_replays(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, status, invocations, replay_claimed = _exercise_world(
                    OperationWorld(job_type, "current", False),
                    beets=self.beets,
                )
                assert_operation_fence(
                    authorized=authorized,
                    terminal_acknowledged=False,
                    final_status=status,
                    beets_invocations=invocations,
                    replay_claimed=replay_claimed,
                )
                self.assertEqual(len(invocations), 1)

    def test_terminal_acknowledgement_prevents_recovery(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, status, invocations, replay_claimed = _exercise_world(
                    OperationWorld(job_type, "current", True),
                    beets=self.beets,
                )
                assert_operation_fence(
                    authorized=authorized,
                    terminal_acknowledged=True,
                    final_status=status,
                    beets_invocations=invocations,
                    replay_claimed=replay_claimed,
                )
                self.assertEqual(
                    status,
                    (
                        IMPORT_JOB_RECOVERY_REQUIRED
                        if job_type == IMPORT_JOB_AUTOMATION
                        else "completed"
                    ),
                )
                self.assertFalse(replay_claimed)


class TestImportOperationFenceChecker(unittest.TestCase):
    def test_checker_rejects_the_old_automatic_replay_policy(self) -> None:
        with self.assertRaisesRegex(AssertionError, "more than once"):
            assert_operation_fence(
                authorized=True,
                terminal_acknowledged=False,
                final_status="queued",
                beets_invocations=[703, 703],
                replay_claimed=True,
            )


if __name__ == "__main__":
    unittest.main()
