"""Generated lifecycle proof for the Beets operation fence (#703)."""

from __future__ import annotations

import os
import re
import tempfile
import unittest
from collections.abc import Callable
from dataclasses import dataclass
from unittest.mock import patch

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.dispatch import DISPATCH_CODE_REQUEUE_FAILED, dispatch_import_core
from lib.dispatch.types import DispatchOutcome, EvidenceImportGate, ImportOneRun
from lib.failure_presentation import non_automation_import_failure_message
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
from lib.pipeline_db.rows import DownloadLogWithEvidenceRow
from lib.quality import DownloadInfo
from lib.quality_evidence import snapshot_audio_files
from scripts.importer import (
    _WORLD_FAILURE_AUDIT_PREFIX,
    _execution_lease_from_job,
    process_claimed_job,
)
from tests.beets_world import BeetsWorld
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_dispatch_request,
    noop_quality_gate,
    pinned_dispatch_authority,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_import_result, make_request_row
from web.download_history_view import build_recents_download_log_rows

# The request id every generated world seeds and self-heal checks read back.
_OPERATION_FENCE_REQUEST_ID = 703
# ``_exercise_world``'s seeded request status. Force/YouTube own no request
# lifecycle, so an ambiguous operation of theirs must leave this untouched.
_SEEDED_REQUEST_STATUS = "wanted"
_NON_AUTOMATION_FAILURE_CLASSES = (
    "executor_crash",
    "bundle_less_failure",
    "requeue_failed",
    "startup_ambiguity",
)
_NON_AUTOMATION_REQUEST_STATUSES = ("wanted", "unsearchable", "imported")


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
    caller left it while one linked failed audit row surfaces the attempt in
    Recents.
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
        # Fail-closed legislation (#1094 clause audit): no current writer
        # creates ``recovery_required``, and a planted revival is refused
        # upstream by ``validate_automation_terminal_authority`` before this
        # clause can see it. Kept so a future writer that bypasses that
        # validation fails loudly here instead of parking silently.
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
        # Fail-closed legislation (#1094 clause audit) for the request-status
        # and owner-pointer clauses immediately below (NOT the audit-row
        # clause after them, which an importer-side mutant does reach): both
        # are decided by the terminal SQL ``_finish_processing_request_last``,
        # which this module stands in for, and every importer-side mutant that
        # declares a non-wanted edge is refused first by
        # ``validate_automation_terminal_authority``. They patrol the DB-layer
        # writer, not this drive path.
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
    else:
        linked = [
            entry for entry in db.download_logs
            if entry.outcome == "failed"
            and entry.source_download_log_id is not None
        ]
        if len(linked) != 1:
            raise AssertionError(
                "non-automation failure recorded no linked Recents audit row"
            )


def assert_startup_force_action_lifecycle(
    *,
    launched: bool,
    final_status: str,
    action_path: str,
) -> None:
    """Terminal recovery removes its copy; retry recovery retains it."""
    action_exists = os.path.exists(action_path)
    if launched:
        if final_status != "failed":
            raise AssertionError(
                f"launched force recovery ended {final_status!r}, want 'failed'"
            )
        if action_exists:
            raise AssertionError(
                "terminal force recovery leaked its private action copy"
            )
        return
    if final_status != "queued":
        raise AssertionError(
            f"unlaunched force recovery ended {final_status!r}, want 'queued'"
        )
    if not action_exists:
        raise AssertionError(
            "retryable force recovery deleted the action copy it still needs"
        )


def assert_non_automation_failure_lifecycle(
    *,
    db: FakePipelineDB,
    job_type: str,
    request_id: int,
    request_status: str,
    source_download_log_id: int,
) -> None:
    """A force/YouTube failure is visible, linked, source-correct and inert."""
    job = next(
        row for row in db._import_jobs
        if row.get("request_id") == request_id and row.get("job_type") == job_type
    )
    if job["status"] != "failed":
        raise AssertionError(f"non-automation job ended {job['status']!r}")
    if db.request(request_id)["status"] != request_status:
        raise AssertionError("non-automation failure changed request lifecycle")
    origin = next(
        row for row in db.download_logs if row.id == source_download_log_id
    )
    if job_type == IMPORT_JOB_YOUTUBE and origin.outcome != "youtube_success":
        # Fail-closed legislation (#1094 clause audit): the canonical handoff
        # is written by ``lib/youtube_ingest_service.py``, which no world here
        # executes — the driver enqueues through the same atomic DB command
        # the service uses. Kept so a future ingest path that enqueues an
        # import without promoting its origin row fails loudly.
        raise AssertionError("YouTube import did not use the canonical handoff")
    failed = [
        row for row in db.download_logs
        if row.outcome == "failed"
        and row.source_download_log_id == source_download_log_id
    ]
    if len(failed) != 1:
        raise AssertionError("non-automation failure did not write one audit row")
    if failed[0].source != origin.source:
        # Fail-closed legislation (#1094 clause audit): the terminal INSERT
        # COALESCEs ``source`` out of the origin row and sets
        # ``source_download_log_id`` from the same subquery, so today the two
        # cannot disagree — dropping the origin trips the clause above first.
        # Kept so a future writer that defaults ``source`` by job type while
        # keeping the link fails loudly.
        raise AssertionError("terminal audit source drifted from its origin")
    audit = db.get_download_log_entry(failed[0].id)
    if audit is None:
        # Fail-closed legislation (#1094 clause audit): the audit row is
        # written inside the terminal transaction against the locked request
        # and its foreign key keeps the joined request alive, so the read-back
        # cannot miss. Kept so a bundle that reports an id it did not commit
        # fails loudly instead of rendering nothing in Recents.
        raise AssertionError("linked terminal audit disappeared")
    rendered = build_recents_download_log_rows([dict(audit)])[0]
    expected_prefix = (
        "Force import attempt failed:"
        if job_type == IMPORT_JOB_FORCE
        else "YouTube import attempt failed:"
    )
    verdict = rendered["verdict"]
    if not isinstance(verdict, str) or not verdict.startswith(expected_prefix):
        raise AssertionError("Recents lost the failed job-type identity")


class AuditReadBackLostDB(FakePipelineDB):
    """Model the one shape that loses a committed terminal audit row.

    ``PipelineDB.get_download_log_entry`` INNER JOINs ``album_requests``
    (``lib/pipeline_db/download_log.py``), so a terminal audit row that a
    bundle claims to have committed can be enumerable and still not read
    back. Today's writers cannot produce that world — the audit row is
    written inside the terminal transaction against the locked request, and
    the foreign key keeps the joined request alive — so this is the only
    world that reaches ``assert_non_automation_failure_lifecycle``'s
    read-back clause. The switch stays off while the world is driven so the
    production path under test sees the ordinary reader.
    """

    audit_read_back_lost: bool = False

    def get_download_log_entry(
        self, log_id: int,
    ) -> DownloadLogWithEvidenceRow | None:
        if self.audit_read_back_lost:
            return None
        return super().get_download_log_entry(log_id)


def _drive_non_automation_failure(
    *,
    job_type: str,
    failure_class: str,
    request_status: str,
    db_factory: Callable[[], FakePipelineDB] = FakePipelineDB,
) -> tuple[FakePipelineDB, int]:
    """Drive each residual path through importer/startup production code."""
    db = db_factory()
    request_id = _OPERATION_FENCE_REQUEST_ID
    db.seed_request(make_request_row(
        id=request_id,
        mb_release_id="generated-non-automation-failure",
        status=(
            "wanted"
            if failure_class == "startup_ambiguity" and request_status == "imported"
            else request_status
        ),
    ))
    if job_type == IMPORT_JOB_FORCE:
        source_download_log_id = db.log_download(request_id, outcome="rejected")
        payload: dict[str, object] = {
            "download_log_id": source_download_log_id,
            "failed_path": "/tmp/generated-non-automation-force",
        }
        launch_source = "/tmp/generated-non-automation-force"
    else:
        source_download_log_id = db.insert_youtube_running(
            request_id=request_id,
            browse_id="MPREb_generated_failure",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/watch?v=generated-failure",
            expected_track_count=1,
        )
        payload = youtube_import_payload(
            staged_path="/tmp/generated-non-automation-youtube",
            request_id=request_id,
            browse_id="MPREb_generated_failure",
            download_log_id=source_download_log_id,
        )
        launch_source = "/tmp/generated-non-automation-youtube"
    dedupe_key = f"{job_type}:{failure_class}:{request_status}:{source_download_log_id}"
    job = (
        db.enqueue_import_job(
            job_type,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=payload,
        )
        if job_type == IMPORT_JOB_FORCE
        else db.enqueue_youtube_import_and_mark_success(
            download_log_id=source_download_log_id,
            request_id=request_id,
            dedupe_key=dedupe_key,
            payload=payload,
            message="generated YouTube rescue staged",
            terminal_metadata={},
        )
    )
    evidence = make_album_quality_evidence(
        mb_release_id="generated-non-automation-failure",
        source_path=launch_source,
    )
    db.upsert_album_quality_evidence(evidence)
    persisted = db.find_album_quality_evidence(
        mb_release_id=evidence.mb_release_id,
        snapshot_fingerprint=evidence.snapshot_fingerprint,
    )
    assert persisted is not None and persisted.id is not None
    assert db.set_import_job_candidate_evidence(job.id, persisted.id)
    db.mark_import_job_preview_importable(job.id, preview_result={})
    claimed = claim_next_import_job(db, worker_id="generated-worker")
    assert claimed is not None

    if failure_class == "startup_ambiguity":
        assert db.authorize_import_job_launch(
            claimed.id,
            request_id=request_id,
            release_id="generated-non-automation-failure",
            source_path=launch_source,
        ) is not None
        if request_status == "imported":
            db.request(request_id)["status"] = "imported"
        db.recover_running_import_jobs(
            requeue_message="generated retry",
            recovery_message="generated startup ambiguity",
        )
        if claim_next_import_job(db, worker_id="replay") is not None:
            raise AssertionError("launched startup ambiguity replayed")
        return db, source_download_log_id

    def execute(*_args: object, **_kwargs: object) -> DispatchOutcome:
        if failure_class == "executor_crash":
            raise RuntimeError("generated executor acknowledgement vanished")
        if failure_class == "bundle_less_failure":
            return DispatchOutcome(False, "generated terminal bundle missing")
        assert failure_class == "requeue_failed"
        return DispatchOutcome(
            False,
            "generated preview requeue update failed",
            code=DISPATCH_CODE_REQUEUE_FAILED,
        )

    process_claimed_job(
        db,  # pyright: ignore[reportArgumentType]
        claimed,
        ctx=object(),
        execute_fn=execute,
    )
    return db, source_download_log_id


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
    source_download_log_id = (
        None
        if world.job_type == IMPORT_JOB_AUTOMATION
        else db.log_download(
            request_id,
            outcome="rejected",
            error_message="generated non-automation source",
        )
        if world.job_type == IMPORT_JOB_FORCE
        else db.insert_youtube_running(
            request_id=request_id,
            browse_id="MPREb_fence",
            audio_playlist_id=None,
            yt_url="https://music.youtube.com/watch?v=generated-fence",
            expected_track_count=1,
        )
    )
    if world.job_type == IMPORT_JOB_AUTOMATION:
        payload: dict[str, object] = {}
    elif world.job_type == IMPORT_JOB_FORCE:
        assert source_download_log_id is not None
        payload = {
            "download_log_id": source_download_log_id,
            "failed_path": source_path,
        }
    else:
        assert source_download_log_id is not None
        payload = youtube_import_payload(
            staged_path=source_path,
            request_id=request_id,
            browse_id="MPREb_fence",
            download_log_id=source_download_log_id,
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
    elif world.job_type == IMPORT_JOB_FORCE:
        job = db.enqueue_import_job(
            world.job_type,
            request_id=request_id,
            dedupe_key=f"{world.job_type}:generated:{request_id}",
            payload=payload,
        )
    else:
        assert source_download_log_id is not None
        job = db.enqueue_youtube_import_and_mark_success(
            download_log_id=source_download_log_id,
            request_id=request_id,
            dedupe_key=f"{world.job_type}:generated:{request_id}",
            payload=payload,
            message="generated YouTube rescue staged",
            terminal_metadata={},
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
            make_dispatch_request(
                path=launch_source,
                mb_release_id=launch_release,
                request_id=request_id,
                label='Generated fence world',
                force=world.job_type == IMPORT_JOB_FORCE,
                beets_harness_path='/nix/store/fake/harness/run_beets_harness.sh',
                dl_info=DownloadInfo(username='generated-peer'),
                distance=0.05,
                scenario='force_import' if world.job_type == IMPORT_JOB_FORCE else 'strong_match',
                candidate_import_job_id=job_arg.id,
                beets_library_db_path=str(beets.library_db),
                beets_library_root=str(beets.library_root),
                execution_lease=execution_lease,
                owner_session_identity=owner_session_identity,
            ),
            db_arg,
            cfg=CratediggerConfig(beets_harness_path='/nix/store/fake/harness/run_beets_harness.sh', pipeline_db_enabled=True),
            quality_gate_fn=noop_quality_gate,
            evidence_gate_fn=lambda *_args, **_kwargs: EvidenceImportGate(candidate=persisted),
            run_import_fn=record_beets_invocation,
            cancellation_token=cancellation_token,
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

    @example(
        job_type=IMPORT_JOB_FORCE,
        failure_class="executor_crash",
        request_status="wanted",
    )
    @example(
        job_type=IMPORT_JOB_YOUTUBE,
        failure_class="bundle_less_failure",
        request_status="unsearchable",
    )
    @example(
        job_type=IMPORT_JOB_FORCE,
        failure_class="requeue_failed",
        request_status="imported",
    )
    @example(
        job_type=IMPORT_JOB_YOUTUBE,
        failure_class="startup_ambiguity",
        request_status="wanted",
    )
    @given(
        job_type=st.sampled_from((IMPORT_JOB_FORCE, IMPORT_JOB_YOUTUBE)),
        failure_class=st.sampled_from(_NON_AUTOMATION_FAILURE_CLASSES),
        request_status=st.sampled_from(_NON_AUTOMATION_REQUEST_STATUSES),
    )
    def test_generated_non_automation_failures_are_visible_and_inert(
        self,
        job_type: str,
        failure_class: str,
        request_status: str,
    ) -> None:
        """All four residual paths preserve source, state, and no replay."""
        db, source_download_log_id = _drive_non_automation_failure(
            job_type=job_type,
            failure_class=failure_class,
            request_status=request_status,
        )
        assert_non_automation_failure_lifecycle(
            db=db,
            job_type=job_type,
            request_id=_OPERATION_FENCE_REQUEST_ID,
            request_status=request_status,
            source_download_log_id=source_download_log_id,
        )

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
                    # The fence checker runs FIRST: the narrow assertions
                    # below are a superset of its "Beets ran without exact
                    # current authority" clause, and running them first
                    # masked that clause's own message (#1094 Q2).
                    assert_operation_fence(
                        job_type=job_type,
                        authorized=authorized,
                        final_status=status,
                        beets_invocations=invocations,
                        replay_claimed=replay_claimed,
                        db=db,
                    )
                    self.assertFalse(authorized)
                    self.assertEqual(invocations, [])

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

    def test_startup_recovery_releases_only_terminal_force_actions(self) -> None:
        """Generated file-count worlds pin startup action-copy ownership."""
        for launched in (False, True):
            interrupted_worlds = (False, True) if launched else (False,)
            for interrupted_after_terminal in interrupted_worlds:
                for file_count in (0, 1, 3):
                    with self.subTest(
                        launched=launched,
                        interrupted_after_terminal=interrupted_after_terminal,
                        file_count=file_count,
                    ), tempfile.TemporaryDirectory() as root:
                        self._assert_startup_force_action_world(
                            root=root,
                            launched=launched,
                            interrupted_after_terminal=(
                                interrupted_after_terminal
                            ),
                            file_count=file_count,
                        )

    def _assert_startup_force_action_world(
        self,
        *,
        root: str,
        launched: bool,
        interrupted_after_terminal: bool,
        file_count: int,
    ) -> None:
        from lib.import_preview import force_action_copy_path
        from scripts import importer

        downloads = os.path.join(root, "downloads")
        processing = os.path.join(root, "processing")
        os.mkdir(downloads, 0o700)
        os.mkdir(processing, 0o700)
        os.mkdir(os.path.join(processing, "albums"), 0o700)
        os.mkdir(os.path.join(processing, "preview"), 0o700)
        cfg = CratediggerConfig(
            slskd_download_dir=downloads,
            processing_dir=processing,
            audio_check_mode="off",
        )
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_OPERATION_FENCE_REQUEST_ID,
            mb_release_id="generated-startup-force",
            status="wanted",
        ))
        source_download_log_id = db.log_download(
            _OPERATION_FENCE_REQUEST_ID,
            outcome="rejected",
        )
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=_OPERATION_FENCE_REQUEST_ID,
            dedupe_key=(
                "force:generated-startup:"
                f"{int(launched)}:{int(interrupted_after_terminal)}:"
                f"{file_count}"
            ),
            payload={
                "download_log_id": source_download_log_id,
                "failed_path": "/failed/generated-startup-force",
            },
        )
        action_path = force_action_copy_path(cfg, job.id)
        os.mkdir(action_path, 0o700)
        for index in range(file_count):
            with open(
                os.path.join(action_path, f"{index:02d}.mp3"),
                "wb",
            ) as handle:
                handle.write(f"audio-{index}".encode())
        evidence = make_album_quality_evidence(
            mb_release_id="generated-startup-force",
            source_path=action_path,
            files=snapshot_audio_files(action_path),
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id=evidence.mb_release_id,
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(
            job.id,
            persisted.id,
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={
                "verdict": "evidence_ready",
                "action_path": action_path,
            },
        )
        claimed = claim_next_import_job(
            db,
            worker_id="generated-old-worker",
        )
        assert claimed is not None
        if launched:
            assert db.authorize_import_job_launch(
                claimed.id,
                request_id=_OPERATION_FENCE_REQUEST_ID,
                release_id="generated-startup-force",
                source_path="/failed/generated-startup-force",
            ) is not None
        if interrupted_after_terminal:
            terminalized = db.recover_running_import_jobs(
                requeue_message="safe retry",
                recovery_message="startup ambiguity",
            )
            assert [item.id for item in terminalized] == [job.id]
            assert terminalized[0].status == "failed"
            assert os.path.exists(action_path)

        with patch(
            "lib.config.read_runtime_config",
            return_value=cfg,
        ):
            importer.recover_abandoned_running_jobs(db)

        final = db.get_import_job(job.id)
        assert final is not None
        assert_startup_force_action_lifecycle(
            launched=launched,
            final_status=final.status,
            action_path=action_path,
        )


def _exact(message: str) -> str:
    """Anchor a clause message so a sibling clause cannot satisfy it.

    ``assertRaisesRegex`` searches, so a bare substring is proof only that
    *some* clause fired. Every known-bad world below names its own clause
    end to end.
    """
    return "^" + re.escape(message) + "$"


class TestImportOperationFenceChecker(unittest.TestCase):
    """Per-clause proof (#1094) for this module's three fence checkers.

    Twenty-one clauses live in ``assert_operation_fence`` (10),
    ``assert_startup_force_action_lifecycle`` (4) and
    ``assert_non_automation_failure_lifecycle`` (7). Each table row below
    names one world that makes THAT clause's condition true while every
    earlier clause in the same function passes, and asserts that clause's
    own message anchored end to end.
    """

    def _db(self, **overrides: object) -> FakePipelineDB:
        overrides.setdefault("status", "wanted")
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=_OPERATION_FENCE_REQUEST_ID, **overrides,
        ))
        return db

    def _db_with_linked_audit(
        self,
        *,
        status: str,
        audits: int = 1,
    ) -> FakePipelineDB:
        """A request whose Recents trail already carries linked failures."""
        db = self._db(status=status)
        source = db.log_download(
            _OPERATION_FENCE_REQUEST_ID,
            outcome="rejected",
        )
        for index in range(audits):
            db.log_download(
                _OPERATION_FENCE_REQUEST_ID,
                outcome="failed",
                source_download_log_id=source,
                error_message=non_automation_import_failure_message(
                    IMPORT_JOB_FORCE,
                    f"generated linked audit {index}",
                ),
            )
        return db

    # ---- assert_operation_fence: clauses 1-10 -------------------------

    def test_operation_fence_clause_worlds(self) -> None:
        """Every ``assert_operation_fence`` clause has a world that fires it."""
        cases: list[tuple[str, Callable[[], None], str]] = [
            (
                "1 one identity reached Beets twice (the old replay policy)",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status="queued",
                    beets_invocations=[703, 703],
                    replay_claimed=True,
                    db=self._db(),
                ),
                "one operation identity reached Beets more than once",
            ),
            (
                "2 Beets ran while the launch fence refused authority",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=False,
                    final_status="queued",
                    beets_invocations=[703],
                    replay_claimed=True,
                    db=self._db(),
                ),
                "Beets ran without exact current authority",
            ),
            (
                "3 the removed pre-#933 recovery_required park revived",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status=IMPORT_JOB_RECOVERY_REQUIRED,
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(
                        status="processing",
                        active_automation_import_job_id=7,
                    ),
                ),
                (
                    f"{IMPORT_JOB_AUTOMATION} ambiguous Beets operation "
                    "parked at 'recovery_required' — CLAUDE.md invariant 11 "
                    "forbids a state whose only exit is an operator command"
                ),
            ),
            (
                "4 an ambiguous operation left a non-terminal queue status",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_FORCE,
                    authorized=True,
                    final_status="queued",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(),
                ),
                (
                    f"{IMPORT_JOB_FORCE} ambiguous Beets operation left job "
                    "status 'queued', want 'failed'"
                ),
            ),
            (
                "5 the terminalized ambiguous job became claimable again",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_FORCE,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=True,
                    db=self._db(),
                ),
                "ambiguous Beets operation became claimable",
            ),
            (
                "6 automation self-heal left the request out of the pool",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(
                        status="processing",
                        active_automation_import_job_id=7,
                    ),
                ),
                (
                    "automation self-heal left request status 'processing', "
                    "want 'wanted' — the request must go back into the "
                    "search pool"
                ),
            ),
            (
                "7 automation self-heal left its owner pointer attached",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(
                        status="wanted",
                        active_automation_import_job_id=7,
                    ),
                ),
                "automation self-heal left the automation owner attached",
            ),
            (
                "8a automation self-heal wrote no audit row at all",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(status="wanted"),
                ),
                (
                    "automation self-heal recorded no world-failure audit "
                    f"row carrying {_WORLD_FAILURE_AUDIT_PREFIX!r}"
                ),
            ),
            (
                "8b the newest audit row carries no world-failure label",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_AUTOMATION,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db_with_linked_audit(status="wanted"),
                ),
                (
                    "automation self-heal recorded no world-failure audit "
                    f"row carrying {_WORLD_FAILURE_AUDIT_PREFIX!r}"
                ),
            ),
            (
                "9 a force job mutated the caller's request lifecycle",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_FORCE,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db_with_linked_audit(status="unsearchable"),
                ),
                (
                    f"{IMPORT_JOB_FORCE} ambiguous operation changed request "
                    "status to 'unsearchable'; force/YouTube own no request "
                    "lifecycle to self-heal"
                ),
            ),
            (
                "10a a terminal force job surfaced nothing in Recents",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_FORCE,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db(),
                ),
                "non-automation failure recorded no linked Recents audit row",
            ),
            (
                "10b one attempt surfaced as two linked Recents rows",
                lambda: assert_operation_fence(
                    job_type=IMPORT_JOB_FORCE,
                    authorized=True,
                    final_status="failed",
                    beets_invocations=[703],
                    replay_claimed=False,
                    db=self._db_with_linked_audit(status="wanted", audits=2),
                ),
                "non-automation failure recorded no linked Recents audit row",
            ),
        ]
        for clause, invoke, message in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, _exact(message),
            ):
                invoke()

    def test_operation_fence_accepts_its_two_early_returns(self) -> None:
        """Must-still-work: the checker's non-violating worlds stay silent."""
        assert_operation_fence(
            job_type=IMPORT_JOB_FORCE,
            authorized=False,
            final_status="queued",
            beets_invocations=[],
            replay_claimed=True,
            db=self._db(),
        )
        assert_operation_fence(
            job_type=IMPORT_JOB_FORCE,
            authorized=True,
            final_status="completed",
            beets_invocations=[703],
            replay_claimed=False,
            db=self._db(),
        )

    # ---- assert_startup_force_action_lifecycle: clauses 11-14 ---------

    def test_startup_force_action_clause_worlds(self) -> None:
        """Every startup action-copy clause has a world that fires it."""
        cases: list[tuple[str, bool, str, bool, str]] = [
            (
                "11 a launched force recovery did not terminalize",
                True, "queued", True,
                "launched force recovery ended 'queued', want 'failed'",
            ),
            (
                "12 a terminal force recovery kept its private copy",
                True, "failed", True,
                "terminal force recovery leaked its private action copy",
            ),
            (
                "13 an unlaunched force recovery terminalized anyway",
                False, "failed", True,
                "unlaunched force recovery ended 'failed', want 'queued'",
            ),
            (
                "14 a retryable force recovery destroyed its own retry input",
                False, "queued", False,
                (
                    "retryable force recovery deleted the action copy it "
                    "still needs"
                ),
            ),
        ]
        for clause, launched, final_status, exists, message in cases:
            with self.subTest(clause=clause), tempfile.TemporaryDirectory() as root:
                action_path = os.path.join(root, "action")
                if exists:
                    os.mkdir(action_path, 0o700)
                with self.assertRaisesRegex(AssertionError, _exact(message)):
                    assert_startup_force_action_lifecycle(
                        launched=launched,
                        final_status=final_status,
                        action_path=action_path,
                    )

    def test_startup_force_action_accepts_both_correct_worlds(self) -> None:
        """Must-still-work: terminal removes its copy, retry retains it."""
        with tempfile.TemporaryDirectory() as root:
            removed = os.path.join(root, "removed")
            retained = os.path.join(root, "retained")
            os.mkdir(retained, 0o700)
            assert_startup_force_action_lifecycle(
                launched=True, final_status="failed", action_path=removed,
            )
            assert_startup_force_action_lifecycle(
                launched=False, final_status="queued", action_path=retained,
            )

    # ---- assert_non_automation_failure_lifecycle: clauses 15-21 -------

    def _driven(
        self,
        *,
        job_type: str = IMPORT_JOB_FORCE,
        db_factory: Callable[[], FakePipelineDB] = FakePipelineDB,
    ) -> tuple[FakePipelineDB, int]:
        return _drive_non_automation_failure(
            job_type=job_type,
            failure_class="bundle_less_failure",
            request_status="wanted",
            db_factory=db_factory,
        )

    def _check_driven(
        self,
        db: FakePipelineDB,
        source_download_log_id: int,
        *,
        job_type: str = IMPORT_JOB_FORCE,
        request_status: str = "wanted",
    ) -> None:
        assert_non_automation_failure_lifecycle(
            db=db,
            job_type=job_type,
            request_id=_OPERATION_FENCE_REQUEST_ID,
            request_status=request_status,
            source_download_log_id=source_download_log_id,
        )

    def _terminal_audit(self, db: FakePipelineDB, source_id: int):
        return next(
            row for row in db.download_logs
            if row.outcome == "failed"
            and row.source_download_log_id == source_id
        )

    def test_non_automation_lifecycle_clause_worlds(self) -> None:
        """Every non-automation lifecycle clause has a world that fires it.

        Each world is a real driven force/YouTube failure — importer,
        terminal-outcome and Recents production code — with exactly one
        planted deviation, so no earlier clause can absorb the violation.
        """
        def clause_15() -> None:
            db, source_id = self._driven()
            job = next(
                row for row in db._import_jobs
                if row.get("request_id") == _OPERATION_FENCE_REQUEST_ID
                and row.get("job_type") == IMPORT_JOB_FORCE
            )
            job["status"] = "completed"
            self._check_driven(db, source_id)

        def clause_16() -> None:
            db, source_id = self._driven()
            db.request(_OPERATION_FENCE_REQUEST_ID)["status"] = "imported"
            self._check_driven(db, source_id)

        def clause_17() -> None:
            db, source_id = self._driven(job_type=IMPORT_JOB_YOUTUBE)
            origin = next(
                row for row in db.download_logs if row.id == source_id
            )
            # The pre-handoff state ``insert_youtube_running`` writes: the
            # enqueue never promoted it to ``youtube_success``.
            origin.outcome = "youtube_running"
            self._check_driven(db, source_id, job_type=IMPORT_JOB_YOUTUBE)

        def clause_18_missing() -> None:
            db, source_id = self._driven()
            db.download_logs.remove(self._terminal_audit(db, source_id))
            self._check_driven(db, source_id)

        def clause_18_duplicated() -> None:
            db, source_id = self._driven()
            db.log_download(
                _OPERATION_FENCE_REQUEST_ID,
                outcome="failed",
                source_download_log_id=source_id,
                error_message=non_automation_import_failure_message(
                    IMPORT_JOB_FORCE, "generated duplicate attempt audit",
                ),
            )
            self._check_driven(db, source_id)

        def clause_19() -> None:
            db, source_id = self._driven(job_type=IMPORT_JOB_YOUTUBE)
            self._terminal_audit(db, source_id).source = "slskd"
            self._check_driven(db, source_id, job_type=IMPORT_JOB_YOUTUBE)

        def clause_20() -> None:
            db, source_id = self._driven(db_factory=AuditReadBackLostDB)
            assert isinstance(db, AuditReadBackLostDB)
            db.audit_read_back_lost = True
            self._check_driven(db, source_id)

        def clause_21() -> None:
            db, source_id = self._driven()
            # The other producer's identity prefix: a real string from
            # ``non_automation_import_failure_message``, wrong for this job.
            self._terminal_audit(db, source_id).error_message = (
                non_automation_import_failure_message(
                    IMPORT_JOB_YOUTUBE, "generated cross-identity diagnostic",
                )
            )
            self._check_driven(db, source_id)

        cases: list[tuple[str, Callable[[], None], str]] = [
            (
                "15 the attempt terminalized as a success, not a failure",
                clause_15,
                "non-automation job ended 'completed'",
            ),
            (
                "16 a force job moved the request's lifecycle",
                clause_16,
                "non-automation failure changed request lifecycle",
            ),
            (
                "17 a YouTube import skipped the canonical success handoff",
                clause_17,
                "YouTube import did not use the canonical handoff",
            ),
            (
                "18a the terminal bundle wrote no linked audit row",
                clause_18_missing,
                "non-automation failure did not write one audit row",
            ),
            (
                "18b one attempt wrote two linked audit rows",
                clause_18_duplicated,
                "non-automation failure did not write one audit row",
            ),
            (
                "19 the terminal audit defaulted its source away from origin",
                clause_19,
                "terminal audit source drifted from its origin",
            ),
            (
                "20 the committed audit row did not read back",
                clause_20,
                "linked terminal audit disappeared",
            ),
            (
                "21 Recents rendered the other job type's failure identity",
                clause_21,
                "Recents lost the failed job-type identity",
            ),
        ]
        for clause, invoke, message in cases:
            with self.subTest(clause=clause), self.assertRaisesRegex(
                AssertionError, _exact(message),
            ):
                invoke()

    def test_non_automation_lifecycle_accepts_the_driven_worlds(self) -> None:
        """Must-still-work: an undisturbed driven failure passes every clause."""
        for job_type in (IMPORT_JOB_FORCE, IMPORT_JOB_YOUTUBE):
            with self.subTest(job_type=job_type):
                db, source_id = self._driven(job_type=job_type)
                self._check_driven(db, source_id, job_type=job_type)


if __name__ == "__main__":
    unittest.main()
