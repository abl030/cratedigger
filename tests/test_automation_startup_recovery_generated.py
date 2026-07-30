"""Generated startup-recovery coverage for mixed import-queue worlds."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import unittest
from typing import Literal

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - loads the active profile

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from lib.import_execution import (
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ProcessIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_RECOVERY_REQUIRED,
    ImportJob,
)
from lib.pipeline_db.cleanup_journal import CleanupJournalIntent
from lib.processing_cleanup import (
    PROCESSING_CLEANUP_REMOVE_SOURCE,
    cleanup_manifest_builtins,
    inspect_processing_cleanup_source,
)
from tests.fakes import FakePipelineDB
from tests.helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
    make_album_quality_evidence,
    make_request_row,
)

Lane = Literal["preview", "import"]
Liveness = Literal["dead", "live", "unknown", "stale"]
JournalState = Literal["absent", "present"]

_REQUEST_ID = 42
_RELEASE_ID = "generated-startup-owner"


def _lease(lane: str) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="generated-old-boot",
        invocation_id=f"generated-{lane}-invocation",
        systemd_unit=f"cratedigger-{lane}.service",
        worker=ProcessIdentity(pid=811, start_ticks=8101),
    )


class _DeadProbe:
    """Every persisted lease belongs to a boot that has ended."""

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="generated-new-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


class _LiveProbe:
    """The persisted worker is still on this boot and still running.

    Composed exactly as the real decision table demands a ``live`` verdict:
    same boot, exact worker identity, exact invocation, exact cgroup.
    """

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        from lib.import_execution import (
            CgroupObservation,
            InvocationObservation,
            ProcessObservation,
        )

        cgroup = f"/system.slice/{lease.systemd_unit}"
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=lease.host_boot_id,
            boot_error=None,
            worker=ProcessObservation(
                lease.worker,
                "exact",
                lease.worker.start_ticks,
                cgroup,
                "pid_exact",
            ),
            beets=None,
            invocation=InvocationObservation(
                "exact",
                lease.invocation_id,
                lease.invocation_id,
                cgroup,
                "invocation_exact",
            ),
            cgroup=CgroupObservation(
                "exact",
                cgroup,
                (lease.worker.pid,),
                "cgroup_exact",
            ),
        )


class _UnknownProbe:
    """The probe itself failed — a transient procfs/systemd read error."""

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        raise OSError("generated transient probe failure")


class _StaleProbe:
    """A confident death verdict, but about a DIFFERENT lease."""

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        return ExecutionLivenessEvidence(
            lease=ExecutionLeaseSnapshot(
                host_boot_id=lease.host_boot_id,
                invocation_id="generated-some-other-invocation",
                systemd_unit=lease.systemd_unit,
                worker=lease.worker,
            ),
            current_host_boot_id="generated-new-boot",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )


_PROBES = {
    "dead": _DeadProbe,
    "live": _LiveProbe,
    "unknown": _UnknownProbe,
    "stale": _StaleProbe,
}


def assert_recovery_never_parks(
    db: FakePipelineDB,
    *,
    request_id: int = _REQUEST_ID,
) -> None:
    """No recovery outcome may leave a request outside the pipeline's reach.

    CLAUDE.md invariant 11: never leave a request in a state whose only exit
    is a human command. After recovery a request is in exactly one of two
    legitimate shapes:

    - ``processing`` with an owner a WORKER will claim (``queued`` or
      ``running``); or
    - out of ``processing`` entirely, with no automation owner attached.

    Anything else is a park: an attached ``recovery_required`` owner, a
    terminal owner still holding the request, or a released request that still
    names one.
    """
    row = db.request(request_id)
    status = row["status"]
    owner_id = row["active_automation_import_job_id"]
    if status != "processing":
        if owner_id is not None:
            raise AssertionError(
                f"request left {status!r} while still naming automation owner "
                f"{owner_id} — the 066 owner equivalence is broken"
            )
        if status not in ("wanted", "unsearchable", "imported"):
            raise AssertionError(
                f"recovery left request in non-runnable status {status!r}"
            )
        return
    if owner_id is None:
        raise AssertionError(
            "request left 'processing' with no owner — the 066 owner "
            "equivalence is broken"
        )
    owner = db.get_import_job(int(owner_id))
    if owner is None:
        raise AssertionError(f"owner {owner_id} does not exist")
    if owner.status == IMPORT_JOB_RECOVERY_REQUIRED:
        raise AssertionError(
            f"owner {owner.id} parked at 'recovery_required' while still "
            f"holding request {request_id} — invariant 11 forbids a state "
            "whose only exit is an operator command"
        )
    if owner.status not in ("queued", "running"):
        raise AssertionError(
            "request 'processing' behind non-claimable owner status "
            f"{owner.status!r}"
        )


def assert_unproven_execution_untouched(
    db: FakePipelineDB,
    *,
    before: ImportJob,
    request_status_before: str,
) -> None:
    """An unproven execution keeps its work; recovery may not steal it."""
    after = db.get_import_job(before.id)
    if after is None:
        raise AssertionError("an unproven owner's job row disappeared")
    if (after.status, after.preview_status) != (
        before.status,
        before.preview_status,
    ):
        raise AssertionError(
            f"unproven execution moved from {before.status}/"
            f"{before.preview_status} to {after.status}/{after.preview_status}"
        )
    if after.execution_invocation_id != before.execution_invocation_id:
        raise AssertionError("unproven execution had its lease cleared")
    row = db.request(_REQUEST_ID)
    if row["status"] != request_status_before:
        raise AssertionError(
            f"unproven execution's request moved to {row['status']!r}"
        )
    if db.download_logs:
        raise AssertionError(
            "unproven execution produced a world-failure audit row"
        )


class _World:
    """One generated abandoned-owner world, built with production writers."""

    def __init__(
        self,
        case: unittest.TestCase,
        *,
        lane: Lane,
        launched: bool,
        journal: JournalState,
    ) -> None:
        root = tempfile.mkdtemp(prefix="generated-startup-recovery-")
        case.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.path = os.path.join(root, "albums", "generated-owner")
        os.makedirs(self.path)
        with open(os.path.join(self.path, "01 - Track.mp3"), "wb") as handle:
            handle.write(b"audio")

        self.db = FakePipelineDB()
        self.db.seed_request(make_request_row(
            id=_REQUEST_ID,
            mb_release_id=_RELEASE_ID,
            status="wanted",
        ))
        owner = handoff_automation_owner(
            self.db,
            _REQUEST_ID,
            canonical_path=self.path,
        )
        self.owner_id = owner.id
        preview_lease = _lease("preview")
        if claim_next_import_preview_job(
            self.db,
            worker_id="generated-preview",
            execution_lease=preview_lease,
        ) is None:
            raise AssertionError("generated preview owner was not claimable")
        self.lease = preview_lease

        if lane == "import":
            evidence = make_album_quality_evidence(
                mb_release_id=_RELEASE_ID,
                source_path=self.path,
            )
            self.db.upsert_album_quality_evidence(evidence)
            persisted = self.db.find_album_quality_evidence(
                mb_release_id=_RELEASE_ID,
                snapshot_fingerprint=evidence.snapshot_fingerprint,
            )
            assert persisted is not None and persisted.id is not None
            assert self.db.set_import_job_candidate_evidence(
                owner.id,
                persisted.id,
                expected_execution_lease=preview_lease,
            )
            if self.db.mark_import_job_preview_importable(
                owner.id,
                expected_execution_lease=preview_lease,
            ) is None:
                raise AssertionError("generated owner did not finish preview")
            import_lease = _lease("importer")
            if claim_next_import_job(
                self.db,
                worker_id="generated-importer",
                execution_lease=import_lease,
            ) is None:
                raise AssertionError("generated import owner not claimable")
            self.lease = import_lease
            if launched and self.db.authorize_import_job_launch(
                owner.id,
                request_id=_REQUEST_ID,
                release_id=_RELEASE_ID,
                source_path=self.path,
                expected_execution_lease=import_lease,
            ) is None:
                raise AssertionError("generated launch was not authorized")

        if journal == "present":
            inspection = inspect_processing_cleanup_source(self.path)
            assert inspection.manifest_hash is not None
            self.db.create_processing_cleanup_journal(
                request_id=_REQUEST_ID,
                job_id=owner.id,
                intent=CleanupJournalIntent(
                    action=PROCESSING_CLEANUP_REMOVE_SOURCE,
                    source_path=self.path,
                    source_manifest=cleanup_manifest_builtins(
                        inspection.manifest
                    ),
                    source_manifest_hash=inspection.manifest_hash,
                ),
            )

    def snapshot(self) -> ImportJob:
        job = self.db.get_import_job(self.owner_id)
        assert job is not None
        return job

    def add_clutter(self, count: int) -> None:
        for index in range(count):
            job = self.db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=_REQUEST_ID,
                dedupe_key=f"force:generated-startup-clutter:{index}",
                payload={
                    "download_log_id": index + 1,
                    "failed_path": f"/tmp/generated-clutter-{index}",
                },
            )
            if self.db.mark_import_job_preview_importable(
                job.id,
                preview_result={"ready": True},
            ) is None:
                raise AssertionError("generated clutter never became ready")


class TestAutomationStartupRecoveryGenerated(unittest.TestCase):
    @given(
        lane=st.sampled_from(("preview", "import")),
        clutter_count=st.integers(min_value=50, max_value=80),
    )
    @example(lane="preview", clutter_count=50)
    @example(lane="import", clutter_count=50)
    def test_exact_owner_survives_every_mixed_timeline_cutoff(
        self,
        *,
        lane: Lane,
        clutter_count: int,
    ) -> None:
        from scripts import import_preview_worker, importer

        world = _World(self, lane=lane, launched=False, journal="absent")
        world.add_clutter(clutter_count)

        # Known-bad implementation: limit a mixed timeline, then filter.
        mutant = [
            job
            for job in world.db.list_import_job_timeline(limit=50)
            if job.job_type == IMPORT_JOB_AUTOMATION
        ]
        self.assertEqual(mutant, [])

        if lane == "preview":
            recovered = import_preview_worker.recover_running_preview_jobs(
                world.db,  # pyright: ignore[reportArgumentType]
                liveness_probe=_DeadProbe(),
            )
        else:
            recovered = importer.recover_abandoned_running_jobs(
                world.db,  # pyright: ignore[reportArgumentType]
                liveness_probe=_DeadProbe(),
            )

        self.assertEqual([job.id for job in recovered], [world.owner_id])

    @settings(deadline=None)
    @given(
        lane=st.sampled_from(("preview", "import")),
        launched=st.booleans(),
        journal=st.sampled_from(("absent", "present")),
        liveness=st.sampled_from(("dead", "live", "unknown", "stale")),
    )
    @example(lane="import", launched=True, journal="absent", liveness="dead")
    @example(lane="import", launched=True, journal="present", liveness="dead")
    @example(
        lane="preview",
        launched=False,
        journal="present",
        liveness="dead",
    )
    @example(lane="preview", launched=False, journal="absent", liveness="dead")
    @example(lane="import", launched=True, journal="present", liveness="live")
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="unknown",
    )
    @example(lane="import", launched=True, journal="present", liveness="stale")
    def test_recovery_never_parks_any_abandoned_owner(
        self,
        *,
        lane: Lane,
        launched: bool,
        journal: JournalState,
        liveness: Liveness,
    ) -> None:
        """Drive the real recovery sweep over every abandoned-owner world."""
        from scripts import importer

        world = _World(self, lane=lane, launched=launched, journal=journal)
        before = world.snapshot()
        request_status_before = str(world.db.request(_REQUEST_ID)["status"])

        importer.recover_abandoned_automation_owners(
            world.db,
            liveness_probe=_PROBES[liveness](),
        )

        assert_recovery_never_parks(world.db)
        if liveness != "dead":
            assert_unproven_execution_untouched(
                world.db,
                before=before,
                request_status_before=request_status_before,
            )

    @settings(deadline=None)
    @given(
        launched=st.booleans(),
        journal=st.sampled_from(("absent", "present")),
    )
    @example(launched=True, journal="present")
    @example(launched=False, journal="absent")
    def test_repeated_reprobe_converges_and_stays_converged(
        self,
        *,
        launched: bool,
        journal: JournalState,
    ) -> None:
        """The sweep is re-runnable: a second pass must not undo the first."""
        from scripts import importer

        world = _World(
            self,
            lane="import" if launched else "preview",
            launched=launched,
            journal=journal,
        )

        for _pass in range(3):
            importer.recover_abandoned_automation_owners(
                world.db,
                liveness_probe=_DeadProbe(),
            )
            assert_recovery_never_parks(world.db)


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: a planted park must be caught."""

    def _automation_row(self, db: FakePipelineDB) -> dict[str, object]:
        return next(
            item
            for item in db._import_jobs
            if item["job_type"] == IMPORT_JOB_AUTOMATION
        )

    def test_attached_recovery_required_owner_is_rejected(self) -> None:
        world = _World(self, lane="import", launched=True, journal="absent")
        self._automation_row(world.db)["status"] = IMPORT_JOB_RECOVERY_REQUIRED

        with self.assertRaises(AssertionError) as caught:
            assert_recovery_never_parks(world.db)

        self.assertIn("recovery_required", str(caught.exception))

    def test_terminal_owner_still_holding_the_request_is_rejected(
        self,
    ) -> None:
        world = _World(self, lane="import", launched=True, journal="absent")
        self._automation_row(world.db)["status"] = "failed"

        with self.assertRaises(AssertionError) as caught:
            assert_recovery_never_parks(world.db)

        self.assertIn("non-claimable owner status", str(caught.exception))

    def test_owner_pointer_left_behind_a_released_request_is_rejected(
        self,
    ) -> None:
        world = _World(self, lane="import", launched=True, journal="absent")
        world.db._requests[_REQUEST_ID]["status"] = "wanted"

        with self.assertRaises(AssertionError) as caught:
            assert_recovery_never_parks(world.db)

        self.assertIn("owner equivalence is broken", str(caught.exception))

    def test_stolen_unproven_execution_is_rejected(self) -> None:
        world = _World(self, lane="import", launched=True, journal="absent")
        before = world.snapshot()
        self._automation_row(world.db)["status"] = "queued"

        with self.assertRaises(AssertionError) as caught:
            assert_unproven_execution_untouched(
                world.db,
                before=before,
                request_status_before="processing",
            )

        self.assertIn("unproven execution moved", str(caught.exception))
