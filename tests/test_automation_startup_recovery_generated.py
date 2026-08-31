"""Generated startup-recovery coverage for mixed import-queue worlds."""

from __future__ import annotations

import os
import re
import shutil
import sys
import tempfile
import unittest
from collections.abc import Callable, Sequence
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
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.evidence_helpers import make_album_quality_evidence
from tests.fakes import FakePipelineDB
from tests.helpers import make_request_row

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

    Clause reachability (issue #1094 audit). The ``recovery_required`` clause
    is patrolled by the historical-owner worlds: deleting the sweep's
    ``recovery_required`` exemption strands the request behind a parked owner
    and fires it. The four owner-equivalence clauses and the missing-owner
    clause are fail-closed legislation — every write that would violate them
    is refused at the database boundary (migration 066's owner-equivalence
    CHECK, mirrored here by a fake that declines to terminalize an automation
    owner outside the atomic owner-aware command), so no production caller
    reaches them today. They stay because this guard legislates for every
    future writer of the owner pointer, not only for the sweep.
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
    """An unproven execution keeps its work; recovery may not steal it.

    Accumulating on purpose (issue #1094 per-clause audit). As a
    short-circuiting chain the job-movement clause masked the three that
    follow it: the one realistic production mutant — a liveness verdict that
    fails open to ``dead`` — moves the job status in EVERY world, so the
    lease, request-status, and audit-row clauses could never be the reported
    violation and none of them had a reachable world. Collecting every
    violation attributes each consequence to the clause that legislates it,
    and each clause keeps its own message on its own line.

    With that mutant planted, the terminal-branch worlds (launched or
    journalled) fire job-moved + request-moved + audit-row and the
    requeue-branch worlds fire job-moved + lease-cleared, so all four clauses
    have a named world. The missing-row precondition below has none: nothing
    in production deletes an import-job row.
    """
    after = db.get_import_job(before.id)
    if after is None:
        # A precondition, not a clause: with no row there is nothing to
        # compare the remaining clauses against.
        raise AssertionError("an unproven owner's job row disappeared")
    violations: list[str] = []
    if (after.status, after.preview_status) != (
        before.status,
        before.preview_status,
    ):
        violations.append(
            f"unproven execution moved from {before.status}/"
            f"{before.preview_status} to {after.status}/{after.preview_status}"
        )
    if after.execution_invocation_id != before.execution_invocation_id:
        violations.append("unproven execution had its lease cleared")
    row = db.request(_REQUEST_ID)
    if row["status"] != request_status_before:
        violations.append(
            f"unproven execution's request moved to {row['status']!r}"
        )
    if db.download_logs:
        violations.append(
            "unproven execution produced a world-failure audit row"
        )
    if violations:
        raise AssertionError("\n".join(violations))


def assert_cleanup_refusal_preserved_tree(
    before: tuple[tuple[str, str, bytes | None], ...],
    after: tuple[tuple[str, str, bytes | None], ...],
) -> None:
    """A cleanup refusal may not alter any remaining filesystem entry."""
    if after != before:
        raise AssertionError(
            "cleanup refusal mutated, renamed, or deleted remaining filesystem"
        )


def _tree_snapshot(root: str) -> tuple[tuple[str, str, bytes | None], ...]:
    entries: list[tuple[str, str, bytes | None]] = []
    for current, directories, files in os.walk(root):
        for name in sorted(directories):
            entries.append((
                os.path.relpath(os.path.join(current, name), root),
                "directory",
                None,
            ))
        for name in sorted(files):
            path = os.path.join(current, name)
            with open(path, "rb") as handle:
                content = handle.read()
            entries.append((os.path.relpath(path, root), "file", content))
    return tuple(sorted(entries))


class _World:
    """One generated abandoned-owner world, built with production writers."""

    def __init__(
        self,
        case: unittest.TestCase,
        *,
        lane: Lane,
        launched: bool,
        journal: JournalState,
        historical: bool = False,
    ) -> None:
        root = tempfile.mkdtemp(prefix="generated-startup-recovery-")
        case.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.root = root
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

        if historical:
            # The one owner shape no current writer can build, applied last
            # because that is the order it arose in: a live owner did its
            # work, then a pre-#933 writer parked it. CLAUDE.md invariant 11
            # retired ``recovery_required`` as a resting state, so rows in it
            # are historical and are seeded directly for exactly that reason.
            # Production still owns them —
            # ``recover_abandoned_automation_owners`` exempts a leaseless
            # ``recovery_required`` owner from its "waiting to be claimed"
            # skip precisely so the sweep converges them automatically, and
            # ``never_claimed`` is their exact death proof.
            row = next(
                item
                for item in self.db._import_jobs
                if item["id"] == owner.id
            )
            row["status"] = IMPORT_JOB_RECOVERY_REQUIRED
            self.db._clear_execution_lease(row)
            self.lease = None

    def make_cleanup_refuse(self, *, payload: bytes) -> None:
        """Drift a real persisted journal so the real executor refuses it."""
        if (
            self.owner_id,
            _REQUEST_ID,
        ) not in self.db._processing_cleanup_journals:
            raise AssertionError("cleanup refusal needs a persisted journal")
        with open(os.path.join(self.path, "foreign.keep"), "wb") as handle:
            handle.write(payload)

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
                world.db,
                liveness_probe=_DeadProbe(),
            )

        self.assertEqual([job.id for job in recovered], [world.owner_id])

    @settings(deadline=None)
    @given(
        lane=st.sampled_from(("preview", "import")),
        launched=st.booleans(),
        journal=st.sampled_from(("absent", "present")),
        liveness=st.sampled_from(("dead", "live", "unknown", "stale")),
        historical=st.booleans(),
    )
    @example(
        lane="import",
        launched=True,
        journal="absent",
        liveness="dead",
        historical=False,
    )
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="dead",
        historical=False,
    )
    @example(
        lane="preview",
        launched=False,
        journal="present",
        liveness="dead",
        historical=False,
    )
    @example(
        lane="preview",
        launched=False,
        journal="absent",
        liveness="dead",
        historical=False,
    )
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="live",
        historical=False,
    )
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="unknown",
        historical=False,
    )
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="stale",
        historical=False,
    )
    # Issue #1094: the two historical-owner worlds the park clause needs.
    # Without them, deleting the ``recovery_required`` exemption in
    # ``recover_abandoned_automation_owners`` — a production fail-open that
    # strands the request behind a parked owner forever — changed nothing any
    # generated world could observe.
    @example(
        lane="import",
        launched=True,
        journal="present",
        liveness="dead",
        historical=True,
    )
    @example(
        lane="preview",
        launched=False,
        journal="absent",
        liveness="live",
        historical=True,
    )
    def test_recovery_never_parks_any_abandoned_owner(
        self,
        *,
        lane: Lane,
        launched: bool,
        journal: JournalState,
        liveness: Liveness,
        historical: bool,
    ) -> None:
        """Drive the real recovery sweep over every abandoned-owner world."""
        from scripts import importer

        world = _World(
            self,
            lane=lane,
            launched=launched,
            journal=journal,
            historical=historical,
        )
        before = world.snapshot()
        request_status_before = str(world.db.request(_REQUEST_ID)["status"])

        importer.recover_abandoned_automation_owners(
            world.db,
            liveness_probe=_PROBES[liveness](),
        )

        assert_recovery_never_parks(world.db)
        if liveness != "dead" and not historical:
            # A historical owner is leaseless, so there is no execution to
            # prove alive: ``never_claimed`` is its exact death proof and the
            # sweep converges it whatever the probe says. Every other world
            # holds a real lease, and an unproven one keeps its work.
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

    @settings(deadline=None)
    @given(
        lane=st.sampled_from(("preview", "import")),
        launched=st.booleans(),
        payload=st.binary(min_size=0, max_size=64),
    )
    @example(lane="preview", launched=False, payload=b"foreign")
    @example(lane="import", launched=True, payload=b"foreign")
    def test_refused_cleanup_preserves_every_remaining_entry_and_converges(
        self,
        *,
        lane: Lane,
        launched: bool,
        payload: bytes,
    ) -> None:
        """Patrol refusal over both worker lanes and varied foreign content."""
        from scripts import importer

        world = _World(
            self,
            lane=lane,
            launched=launched,
            journal="present",
        )
        world.make_cleanup_refuse(payload=payload)
        before = _tree_snapshot(world.root)

        recovered = importer.recover_abandoned_automation_owners(
            world.db,
            liveness_probe=_DeadProbe(),
        )

        self.assertEqual([job.id for job in recovered], [world.owner_id])
        assert_recovery_never_parks(world.db)
        assert_cleanup_refusal_preserved_tree(
            before,
            _tree_snapshot(world.root),
        )
        job = world.db.get_import_job(world.owner_id)
        assert job is not None and job.result is not None
        cleanup = job.result["processing_cleanup"]
        assert isinstance(cleanup, dict)
        self.assertEqual(cleanup["outcome"], "refused")
        self.assertEqual(cleanup["disposition"], "left_in_place")
        self.assertEqual(cleanup["error_code"], "manifest_drift")
        self.assertNotIn(
            (world.owner_id, _REQUEST_ID),
            world.db._processing_cleanup_journals,
        )


_ClauseCase = tuple[str, Callable[[], tuple[str, Callable[[], None]]]]


class TestInvariantCheckersTripOnViolations(unittest.TestCase):
    """Known-bad self-tests: one named world per clause (issue #1094).

    Every clause of every checker in this module names the minimal world that
    makes it fire while each earlier clause in the same function passes, and
    each assertion is anchored to that clause's own complete message. A bare
    ``assertRaises(AssertionError)`` proved nothing: the previous five
    self-tests matched substrings, and two of those substrings
    (``owner equivalence is broken``) belong to two different clauses, so a
    message-collision mutant would have survived both. Each table therefore
    also proves that no clause's pattern matches any sibling clause's message
    **within that checker** — ``_prove_clauses`` cross-checks the cases it is
    given, which is one checker per call, not across the module's three
    (round-3 review N4).

    Deterministic by policy — this is test machinery, never a generated
    subject.
    """

    def _world(self) -> _World:
        return _World(self, lane="import", launched=True, journal="absent")

    def _automation_row(self, db: FakePipelineDB) -> dict[str, object]:
        return next(
            item
            for item in db._import_jobs
            if item["job_type"] == IMPORT_JOB_AUTOMATION
        )

    def _prove_clauses(self, cases: Sequence[_ClauseCase]) -> None:
        """Fire each clause on its own world and pin its exact message."""
        produced: dict[str, str] = {}
        patterns: dict[str, str] = {}
        for name, build in cases:
            with self.subTest(clause=name):
                expected, call = build()
                pattern = "(?m)^" + re.escape(expected) + "$"
                patterns[name] = pattern
                with self.assertRaisesRegex(AssertionError, pattern) as caught:
                    call()
                produced[name] = str(caught.exception)
        for name, message in produced.items():
            matched = sorted(
                other
                for other, pattern in patterns.items()
                if re.search(pattern, message)
            )
            self.assertEqual(
                matched,
                [name],
                f"clause {name}'s message is also matched by {matched} — a "
                "sibling clause could satisfy this test's assertion",
            )

    def test_every_recovery_park_clause_fires_with_its_own_message(
        self,
    ) -> None:
        def released_request_still_names_owner() -> tuple[
            str, Callable[[], None]
        ]:
            world = self._world()
            world.db._requests[_REQUEST_ID]["status"] = "wanted"
            return (
                (
                    f"request left 'wanted' while still naming automation "
                    f"owner {world.owner_id} — the 066 owner equivalence is "
                    f"broken"
                ),
                lambda: assert_recovery_never_parks(world.db),
            )

        def released_request_left_non_runnable() -> tuple[
            str, Callable[[], None]
        ]:
            world = self._world()
            request = world.db._requests[_REQUEST_ID]
            request["status"] = "downloading"
            request["active_automation_import_job_id"] = None
            return (
                "recovery left request in non-runnable status 'downloading'",
                lambda: assert_recovery_never_parks(world.db),
            )

        def processing_without_owner() -> tuple[str, Callable[[], None]]:
            world = self._world()
            world.db._requests[
                _REQUEST_ID
            ]["active_automation_import_job_id"] = None
            return (
                (
                    "request left 'processing' with no owner — the 066 owner "
                    "equivalence is broken"
                ),
                lambda: assert_recovery_never_parks(world.db),
            )

        def owner_pointer_names_no_row() -> tuple[str, Callable[[], None]]:
            world = self._world()
            missing = world.owner_id + 5000
            world.db._requests[
                _REQUEST_ID
            ]["active_automation_import_job_id"] = missing
            return (
                f"owner {missing} does not exist",
                lambda: assert_recovery_never_parks(world.db),
            )

        def owner_parked_at_recovery_required() -> tuple[
            str, Callable[[], None]
        ]:
            world = self._world()
            self._automation_row(
                world.db
            )["status"] = IMPORT_JOB_RECOVERY_REQUIRED
            return (
                (
                    f"owner {world.owner_id} parked at 'recovery_required' "
                    f"while still holding request {_REQUEST_ID} — invariant "
                    "11 forbids a state whose only exit is an operator command"
                ),
                lambda: assert_recovery_never_parks(world.db),
            )

        def terminal_owner_still_holds_request() -> tuple[
            str, Callable[[], None]
        ]:
            world = self._world()
            self._automation_row(world.db)["status"] = "failed"
            return (
                (
                    "request 'processing' behind non-claimable owner status "
                    "'failed'"
                ),
                lambda: assert_recovery_never_parks(world.db),
            )

        self._prove_clauses((
            ("released_request_still_names_owner",
             released_request_still_names_owner),
            ("released_request_left_non_runnable",
             released_request_left_non_runnable),
            ("processing_without_owner", processing_without_owner),
            ("owner_pointer_names_no_row", owner_pointer_names_no_row),
            ("owner_parked_at_recovery_required",
             owner_parked_at_recovery_required),
            ("terminal_owner_still_holds_request",
             terminal_owner_still_holds_request),
        ))

    def test_every_unproven_execution_clause_fires_with_its_own_message(
        self,
    ) -> None:
        def job_row_disappeared() -> tuple[str, Callable[[], None]]:
            world = self._world()
            before = world.snapshot()
            world.db._import_jobs.remove(self._automation_row(world.db))
            return (
                "an unproven owner's job row disappeared",
                lambda: assert_unproven_execution_untouched(
                    world.db,
                    before=before,
                    request_status_before="processing",
                ),
            )

        def execution_moved() -> tuple[str, Callable[[], None]]:
            world = self._world()
            before = world.snapshot()
            self._automation_row(world.db)["status"] = "queued"
            return (
                (
                    f"unproven execution moved from {before.status}/"
                    f"{before.preview_status} to queued/"
                    f"{before.preview_status}"
                ),
                lambda: assert_unproven_execution_untouched(
                    world.db,
                    before=before,
                    request_status_before="processing",
                ),
            )

        def lease_cleared() -> tuple[str, Callable[[], None]]:
            world = self._world()
            before = world.snapshot()
            self._automation_row(world.db)["execution_invocation_id"] = None
            return (
                "unproven execution had its lease cleared",
                lambda: assert_unproven_execution_untouched(
                    world.db,
                    before=before,
                    request_status_before="processing",
                ),
            )

        def request_moved() -> tuple[str, Callable[[], None]]:
            world = self._world()
            before = world.snapshot()
            world.db._requests[_REQUEST_ID]["status"] = "wanted"
            return (
                "unproven execution's request moved to 'wanted'",
                lambda: assert_unproven_execution_untouched(
                    world.db,
                    before=before,
                    request_status_before="processing",
                ),
            )

        def world_failure_audit_written() -> tuple[str, Callable[[], None]]:
            world = self._world()
            before = world.snapshot()
            world.db.log_download(
                request_id=_REQUEST_ID,
                outcome="failed",
                error_message="planted world-failure audit row",
            )
            return (
                "unproven execution produced a world-failure audit row",
                lambda: assert_unproven_execution_untouched(
                    world.db,
                    before=before,
                    request_status_before="processing",
                ),
            )

        self._prove_clauses((
            ("job_row_disappeared", job_row_disappeared),
            ("execution_moved", execution_moved),
            ("lease_cleared", lease_cleared),
            ("request_moved", request_moved),
            ("world_failure_audit_written", world_failure_audit_written),
        ))

    def test_refusal_clause_fires_with_its_own_message(self) -> None:
        def deleted_remaining_file() -> tuple[str, Callable[[], None]]:
            before = (("albums/owner/01.flac", "file", b"audio"),)
            return (
                (
                    "cleanup refusal mutated, renamed, or deleted remaining "
                    "filesystem"
                ),
                lambda: assert_cleanup_refusal_preserved_tree(before, ()),
            )

        self._prove_clauses((
            ("deleted_remaining_file", deleted_remaining_file),
        ))
