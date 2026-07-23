"""Generated lifecycle proof for the Beets operation fence (#703)."""

from __future__ import annotations

from dataclasses import dataclass
import os
import unittest
from unittest.mock import patch

from hypothesis import given, strategies as st

from lib.config import CratediggerConfig
from lib.dispatch import dispatch_import_core
from lib.dispatch.types import EvidenceImportGate, ImportOneRun
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    IMPORT_JOB_YOUTUBE,
    youtube_import_payload,
)
from lib.quality import DownloadInfo
from scripts.importer import process_claimed_job
import tests._hypothesis_profiles  # noqa: F401
from tests.fakes import FakePipelineDB
from tests.beets_world import BeetsWorld
from tests.helpers import (
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
)


@dataclass(frozen=True)
class OperationWorld:
    job_type: str
    authority: str
    terminal_acknowledged: bool


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
    request_status = (
        "downloading"
        if world.job_type == IMPORT_JOB_AUTOMATION
        else "wanted"
    )
    active_state = (
        {"current_path": source_path, "files": []}
        if world.job_type == IMPORT_JOB_AUTOMATION
        else None
    )
    db.seed_request(make_request_row(
        id=request_id,
        mb_release_id=release_id,
        status=request_status,
        active_download_state=active_state,
    ))
    if world.job_type == IMPORT_JOB_AUTOMATION:
        payload: dict[str, object] = {}
    elif world.job_type == IMPORT_JOB_FORCE:
        payload = {"failed_path": source_path}
    else:
        payload = youtube_import_payload(
            staged_path=source_path,
            request_id=request_id,
            browse_id="MPREb_fence",
        )
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
    db.set_import_job_candidate_evidence(job.id, persisted.id)
    db.mark_import_job_preview_importable(job.id, preview_result={"ready": True})
    claimed = db.claim_next_import_job(worker_id="generated-worker")
    assert claimed is not None

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

    def execute(db_arg, job_arg, *, ctx=None):
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
        )

    if world.authority != "not_executed":
        process_claimed_job(
            db,  # type: ignore[arg-type]
            claimed,
            execute_fn=execute,
        )
    launched_job = db.get_import_job(claimed.id)
    assert launched_job is not None
    authorized = launched_job.beets_launch_authorized_at is not None

    db.recover_running_import_jobs(
        requeue_message="proven unstarted",
        recovery_message="operator recovery required",
    )
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
                self.beets.root / "poisoned-runtime-config.ini"
            ),
            "BEETS_DB": str(self.beets.root / "poisoned-library.db"),
        })
        self.runtime.start()
        self.addCleanup(self.runtime.stop)

    @given(
        job_type=st.sampled_from([
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ]),
        stale_dimension=st.sampled_from([
            "release_changed",
            "status_changed",
            "source_changed",
        ]),
    )
    def test_stale_authority_never_launches_beets(
        self,
        job_type: str,
        stale_dimension: str,
    ) -> None:
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

    @given(job_type=st.sampled_from([
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_FORCE,
        IMPORT_JOB_YOUTUBE,
    ]))
    def test_definitely_not_started_recovery_may_retry(self, job_type: str) -> None:
        authorized, _status, invocations, replay_claimed = _exercise_world(
            OperationWorld(job_type, "not_executed", False),
            beets=self.beets,
        )
        self.assertFalse(authorized)
        self.assertTrue(replay_claimed)
        self.assertEqual(invocations, [])

    @given(job_type=st.sampled_from([
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_FORCE,
        IMPORT_JOB_YOUTUBE,
    ]))
    def test_may_have_started_recovery_never_replays(self, job_type: str) -> None:
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

    @given(job_type=st.sampled_from([
        IMPORT_JOB_AUTOMATION,
        IMPORT_JOB_FORCE,
        IMPORT_JOB_YOUTUBE,
    ]))
    def test_terminal_acknowledgement_prevents_recovery(self, job_type: str) -> None:
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
        self.assertEqual(status, "completed")
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
