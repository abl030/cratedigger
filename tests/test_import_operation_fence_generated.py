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
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessEvidence,
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
from scripts.importer import (
    _WORLD_FAILURE_AUDIT_PREFIX,
    _execution_lease_from_job,
    process_claimed_job,
)
from tests.beets_world import BeetsWorld
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_import_result,
    make_request_row,
    noop_quality_gate,
    pinned_dispatch_authority,
)

# The request id every generated world seeds and self-heal checks read back.
_OPERATION_FENCE_REQUEST_ID = 703
# ``_exercise_world``'s seeded request status. Force/YouTube own no request
# lifecycle, so an ambiguous operation of theirs must leave this untouched.
_SEEDED_REQUEST_STATUS = "wanted"


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
    job_type: str,
    authorized: bool,
    final_status: str,
    beets_invocations: list[int],
    replay_claimed: bool,
    db: FakePipelineDB,
    request_id: int = _OPERATION_FENCE_REQUEST_ID,
) -> None:
    """Every ambiguous authorized operation stops before an automatic replay.

    Ambiguity is read from what actually landed (``final_status``), not from
    the caller's intent: a launch whose completion could not be positively
    captured is ambiguous regardless of whether the Beets child itself
    returned a clean result — a fake harness that never identifies its own
    child (no ``on_spawn``) is exactly such a world, and it is exactly the
    one the exact-owner completion capture exists to distrust.

    CLAUDE.md invariant 11 ("broken worlds surface and restart, nothing is
    ever parked") retired ``recovery_required`` as a resting state for an
    ambiguous authorized operation — the removed policy stopped there for a
    human command, which is exactly the park this now forbids. The owner job
    instead terminalizes ``failed``. Automation additionally self-heals: its
    request returns to the search pool with its owner cleared, and one
    ``download_log`` audit row carries the world-failure label so the
    operator reads it in Recents. Force/YouTube own no request lifecycle, so
    an ambiguous operation of theirs leaves the request exactly as the
    caller left it — the terminal job row alone is that outcome's surface.
    """
    if len(beets_invocations) > 1:
        raise AssertionError("one operation identity reached Beets more than once")
    if not authorized:
        if beets_invocations:
            raise AssertionError("Beets ran without exact current authority")
        return
    if final_status == "completed":
        # A genuine, positively-captured success. Nothing ambiguous to fence.
        return
    if final_status == IMPORT_JOB_RECOVERY_REQUIRED:
        raise AssertionError(
            f"{job_type} ambiguous Beets operation parked at "
            "'recovery_required' — CLAUDE.md invariant 11 forbids a state "
            "whose only exit is an operator command"
        )
    if final_status != "failed":
        raise AssertionError(
            f"{job_type} ambiguous Beets operation left job status "
            f"{final_status!r}, want 'failed'"
        )
    if replay_claimed:
        raise AssertionError("ambiguous Beets operation became claimable")
    row = db.request(request_id)
    if job_type == IMPORT_JOB_AUTOMATION:
        if row["status"] != "wanted":
            raise AssertionError(
                f"automation self-heal left request status {row['status']!r}, "
                "want 'wanted' — the request must go back into the search pool"
            )
        if row["active_automation_import_job_id"] is not None:
            raise AssertionError(
                "automation self-heal left the automation owner attached"
            )
        logs = db.download_logs
        detail = " ".join(
            part for part in (
                logs[-1].beets_detail if logs else None,
                logs[-1].error_message if logs else None,
            ) if part
        )
        if not logs or _WORLD_FAILURE_AUDIT_PREFIX not in detail:
            raise AssertionError(
                "automation self-heal recorded no world-failure audit row "
                f"carrying {_WORLD_FAILURE_AUDIT_PREFIX!r}"
            )
    elif row["status"] != _SEEDED_REQUEST_STATUS:
        raise AssertionError(
            f"{job_type} ambiguous operation changed request status to "
            f"{row['status']!r}; force/YouTube own no request lifecycle to "
            "self-heal"
        )


def _exercise_world(
    world: OperationWorld,
    *,
    beets: BeetsWorld,
) -> tuple[bool, str, list[int], bool, FakePipelineDB]:
    db = FakePipelineDB()
    request_id = _OPERATION_FENCE_REQUEST_ID
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
        preview_claim = claim_next_import_preview_job(db, worker_id="generated-preview-worker",
        execution_lease=preview_lease,)
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
    claimed = claim_next_import_job(db, worker_id="generated-worker",
    execution_lease=importer_lease,)
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
        replay = claim_next_import_job(db, worker_id="automatic-import-replay",
        execution_lease=_execution_lease(claimed.id, lane="importer-replay"),)
    else:
        replay = claim_next_import_job(db, worker_id="automatic-replay")
    replay_claimed = replay is not None

    final = db.get_import_job(claimed.id)
    assert final is not None
    return authorized, final.status, beets_invocations, replay_claimed, db


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
                    authorized, status, invocations, replay_claimed, db = (
                        _exercise_world(world, beets=self.beets)
                    )
                    self.assertFalse(authorized)
                    self.assertEqual(invocations, [])
                    assert_operation_fence(
                        job_type=job_type,
                        authorized=authorized,
                        final_status=status,
                        beets_invocations=invocations,
                        replay_claimed=replay_claimed,
                        db=db,
                    )

    def test_definitely_not_started_recovery_may_retry(self) -> None:
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, _status, invocations, replay_claimed, _db = (
                    _exercise_world(
                        OperationWorld(job_type, "not_executed", False),
                        beets=self.beets,
                    )
                )
                self.assertFalse(authorized)
                self.assertTrue(replay_claimed)
                self.assertEqual(invocations, [])

    def test_may_have_started_self_heals_never_replays(self) -> None:
        """A launched-then-ambiguous operation self-heals (CLAUDE.md
        invariant 11) instead of parking at ``recovery_required``, and the
        self-healed job can never be automatically replay-claimed."""
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, status, invocations, replay_claimed, db = (
                    _exercise_world(
                        OperationWorld(job_type, "current", False),
                        beets=self.beets,
                    )
                )
                assert_operation_fence(
                    job_type=job_type,
                    authorized=authorized,
                    final_status=status,
                    beets_invocations=invocations,
                    replay_claimed=replay_claimed,
                    db=db,
                )
                self.assertEqual(len(invocations), 1)
                self.assertEqual(status, "failed")

    def test_terminal_acknowledgement_never_replays(self) -> None:
        """Whatever an acknowledged operation terminalizes to, it is never
        automatically replay-claimed. For automation this world is STILL
        ambiguous — the fake Beets child is never positively identified
        (no ``on_spawn``), so the exact-owner completion capture correctly
        distrusts it and self-heals to ``failed`` exactly like the
        launched-then-ambiguous world above; force/YouTube own no request
        lifecycle and terminalize a genuine ``completed``."""
        for job_type in (
            IMPORT_JOB_AUTOMATION,
            IMPORT_JOB_FORCE,
            IMPORT_JOB_YOUTUBE,
        ):
            with self.subTest(job_type=job_type):
                authorized, status, invocations, replay_claimed, db = (
                    _exercise_world(
                        OperationWorld(job_type, "current", True),
                        beets=self.beets,
                    )
                )
                assert_operation_fence(
                    job_type=job_type,
                    authorized=authorized,
                    final_status=status,
                    beets_invocations=invocations,
                    replay_claimed=replay_claimed,
                    db=db,
                )
                self.assertEqual(
                    status,
                    "failed" if job_type == IMPORT_JOB_AUTOMATION else "completed",
                )
                self.assertFalse(replay_claimed)


class TestImportOperationFenceChecker(unittest.TestCase):
    def _db(self, **overrides: object) -> FakePipelineDB:
        overrides.setdefault("status", "wanted")
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_OPERATION_FENCE_REQUEST_ID, **overrides,
        ))
        return db

    def test_checker_rejects_the_old_automatic_replay_policy(self) -> None:
        with self.assertRaisesRegex(AssertionError, "more than once"):
            assert_operation_fence(
                job_type=IMPORT_JOB_AUTOMATION,
                authorized=True,
                final_status="queued",
                beets_invocations=[703, 703],
                replay_claimed=True,
                db=self._db(),
            )

    def test_checker_rejects_planted_recovery_required_park(self) -> None:
        """Known-bad: reviving the removed ``recovery_required`` rest state
        (the pre-#933 policy this file used to require) trips the checker."""
        with self.assertRaisesRegex(AssertionError, "recovery_required"):
            assert_operation_fence(
                job_type=IMPORT_JOB_AUTOMATION,
                authorized=True,
                final_status=IMPORT_JOB_RECOVERY_REQUIRED,
                beets_invocations=[703],
                replay_claimed=False,
                db=self._db(
                    status="processing", active_automation_import_job_id=7,
                ),
            )

    def test_checker_rejects_replay_after_ambiguous_operation(self) -> None:
        """Known-bad: an ambiguous operation whose job became claimable again."""
        with self.assertRaisesRegex(AssertionError, "became claimable"):
            assert_operation_fence(
                job_type=IMPORT_JOB_FORCE,
                authorized=True,
                final_status="failed",
                beets_invocations=[703],
                replay_claimed=True,
                db=self._db(),
            )

    def test_checker_rejects_automation_self_heal_with_owner_attached(self) -> None:
        """Known-bad: self-heal that failed to clear the automation owner."""
        with self.assertRaisesRegex(AssertionError, "owner attached"):
            assert_operation_fence(
                job_type=IMPORT_JOB_AUTOMATION,
                authorized=True,
                final_status="failed",
                beets_invocations=[703],
                replay_claimed=False,
                db=self._db(
                    status="wanted", active_automation_import_job_id=7,
                ),
            )

    def test_checker_rejects_automation_self_heal_missing_audit_row(self) -> None:
        """Known-bad: a silent self-heal with no Recents-visible trace."""
        with self.assertRaisesRegex(AssertionError, "world-failure audit row"):
            assert_operation_fence(
                job_type=IMPORT_JOB_AUTOMATION,
                authorized=True,
                final_status="failed",
                beets_invocations=[703],
                replay_claimed=False,
                db=self._db(status="wanted"),
            )

    def test_checker_rejects_force_import_self_healing_the_request(self) -> None:
        """Known-bad: a force/YouTube job that mutated the caller's request
        lifecycle — invariant 11 reserves that self-heal for automation,
        which is the only job type that owns ``processing``."""
        with self.assertRaisesRegex(AssertionError, "own no request lifecycle"):
            assert_operation_fence(
                job_type=IMPORT_JOB_FORCE,
                authorized=True,
                final_status="failed",
                beets_invocations=[703],
                replay_claimed=False,
                db=self._db(status="unsearchable"),
            )


if __name__ == "__main__":
    unittest.main()
