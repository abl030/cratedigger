"""Self-tests for the FakePipelineDB import-job lane cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest
from datetime import timedelta

from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from tests.dispatch_helpers import (
    claim_next_import_job,
    claim_next_import_preview_job,
    handoff_automation_owner,
)
from tests.fakes import (
    FakePipelineDB,
)
from tests.helpers import (
    make_request_row,
)


class TestFakeActiveImportJobsForWrongMatch(unittest.TestCase):
    def test_matches_by_download_log_path_or_source_dir(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        for request_id in (1, 2, 3, 42):
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
        by_log = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=1,
            payload=force_import_payload(download_log_id=10, failed_path="/other"),
        )
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=11, failed_path="/other"),
        )
        by_path = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=2,
            payload=force_import_payload(download_log_id=12, failed_path="/failed/a"),
        )
        by_dir = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=3,
            payload=force_import_payload(
                download_log_id=13,
                failed_path="/other",
                source_dirs=["alice\\Album"],
            ),
        )
        ignored = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=14, failed_path="/failed/a"),
        )
        completed = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            payload=force_import_payload(download_log_id=15, failed_path="/failed/a"),
        )
        db.mark_import_job_preview_importable(
            completed.id,
            preview_result={"verdict": "would_import"},
            message="ok",
        )
        claimed = claim_next_import_job(db, worker_id="w")
        assert claimed is not None
        db.mark_import_job_completed(claimed.id, result={"ok": True})

        rows = db.list_active_import_jobs_for_wrong_match(
            download_log_id=10,
            request_id=42,
            failed_paths=["/failed/a"],
            source_dirs=["alice\\Album"],
            ignore_import_job_id=ignored.id,
        )

        self.assertEqual(
            {job.id for job in rows},
            {by_log.id, by_path.id, by_dir.id},
        )


class TestFakeClaimMirrorsProductionsLaneGuards(unittest.TestCase):
    """Guards the fake used to be more permissive about than production.

    Issue #1313's mutant rounds found each of these the same way: a
    condition production's SQL spells, the fake spells too, and no
    fake-driven test observed. Production's claim SQL binds the caller's
    ``request_id`` in the job guard and NULLs the preview lane's
    ``cleared_columns``; its candidate scan refuses a force or local job
    whose request has moved out from under it.
    """

    def _owner(self, db: FakePipelineDB, request_id: int):
        db.seed_request(make_request_row(
            id=request_id,
            mb_release_id=f"fake-lane-guard-{request_id}",
            status="wanted",
        ))
        return handoff_automation_owner(db, request_id)

    def test_an_automation_claim_naming_another_request_is_refused(
        self,
    ) -> None:
        """The owner pointer alone must not admit a mismatched request id.

        ``_automation_job_has_authority`` reads only the row's own owner
        state, so without the caller-supplied ``request_id`` in the row
        guard the fake would claim here while production's SQL refuses.
        """
        db = FakePipelineDB()
        job = self._owner(db, 4401)
        bystander = self._owner(db, 4402)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-lane-guard",
            invocation_id="invocation-lane-guard",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=601, start_ticks=6001),
        )
        self.assertIsNone(
            db.claim_automation_import_preview_job_under_lock(
                job.id,
                request_id=bystander.request_id or 0,
                worker_id="preview",
                execution_lease=lease,
            ),
        )
        self.assertIsNone(
            db.claim_automation_import_job_under_lock(
                job.id,
                request_id=bystander.request_id or 0,
                worker_id="importer",
                execution_lease=lease,
            ),
        )
        # ... and the exact request still claims it.
        self.assertIsNotNone(
            db.claim_automation_import_preview_job_under_lock(
                job.id,
                request_id=job.request_id or 0,
                worker_id="preview",
                execution_lease=lease,
            ),
        )

    def test_a_preview_claim_clears_the_previous_attempts_message(
        self,
    ) -> None:
        """The real worker-restart sequence, driven through the fake."""
        from lib.import_queue import IMPORT_JOB_YOUTUBE

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4403,
            mb_release_id="fake-lane-clear",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=4403,
            payload={
                "staged_path": "/incoming/album",
                "request_id": 4403,
                "browse_id": "MPREb_lane",
                "download_log_id": 1,
            },
        )
        self.assertIsNotNone(
            db.claim_import_preview_job_candidate(job.id, worker_id="first"),
        )
        requeued = db.requeue_running_import_preview_jobs(
            message="worker restarted mid-preview",
        )
        self.assertEqual([row.id for row in requeued], [job.id])
        requeued_row = db.get_import_job(job.id)
        assert requeued_row is not None
        self.assertEqual(
            requeued_row.preview_message, "worker restarted mid-preview",
        )
        reclaimed = db.claim_import_preview_job_candidate(
            job.id, worker_id="second",
        )
        assert reclaimed is not None
        # ``preview_message`` is the only half this constrains: the requeue
        # already NULLs ``preview_error``, so asserting it here would pin a
        # bystander.
        self.assertIsNone(reclaimed.preview_message)

    def test_a_force_candidate_whose_request_drifted_is_not_offered(
        self,
    ) -> None:
        """Both lanes' candidate scans read one routing rule, so pin both.

        Production's ``_CANDIDATE_JOB_TYPE_ROUTING`` requires the request to
        still be sitting at the job's ``expected_request_status``, and the
        fake's ``_candidate_job_type_routes`` says the same thing. Nothing
        drove it: with the whole force/local arm replaced by ``return True``,
        every module that reaches a candidate scan stayed green —
        tests.test_import_queue, tests.test_importer_job_kinds,
        tests.test_fakes, tests.test_fakes_import_jobs,
        tests.test_local_import_lane, tests.test_import_operation_fence and
        tests.test_import_job_lane_generated.
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4404,
            mb_release_id="fake-lane-drift",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=4404,
            payload=force_import_payload(
                download_log_id=4404, failed_path="/failed/drift",
            ),
        )

        def preview_offers() -> bool:
            return any(
                row.id == job.id
                for row in db.peek_import_preview_job_candidates(limit=10)
            )

        def import_offers() -> bool:
            return any(
                row.id == job.id
                for row in db.peek_import_job_candidates(limit=10)
            )

        # Must still work: at the expected status the fresh job is a preview
        # candidate, so the refusal below is a refusal and not a fixture that
        # never qualified.
        self.assertTrue(preview_offers())
        db.update_status(4404, "imported")
        self.assertFalse(preview_offers())

        # Same rule, other lane. The import scan needs an importable preview
        # status first, which is the one thing that differs between the two
        # call sites.
        db.update_status(4404, "wanted")
        db.mark_import_job_preview_importable(job.id)
        self.assertTrue(import_offers())
        db.update_status(4404, "imported")
        self.assertFalse(import_offers())

    def _lease(self, beets: bool = False) -> ExecutionLeaseSnapshot:
        return ExecutionLeaseSnapshot(
            host_boot_id="boot-lane-scan",
            invocation_id="invocation-lane-scan",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=611, start_ticks=6011),
            beets=ProcessIdentity(pid=612, start_ticks=6012) if beets else None,
        )

    def test_an_automation_candidate_needs_a_lease(self) -> None:
        """The routing rule's other guarded arm, in both lanes.

        Production gates automation candidates on the caller having an
        execution lease at all (``%s IS NOT NULL`` in the fragment). The
        force/local arm above was pinned while this one was not: replacing
        the whole automation arm with ``True`` left every module that
        reaches a candidate scan green.
        """
        from lib.pipeline_db._shared import ADVISORY_LOCK_NAMESPACE_IMPORT

        db = FakePipelineDB()
        job = self._owner(db, 4406)
        lease = self._lease()

        def preview_offers(execution_lease: ExecutionLeaseSnapshot | None) -> bool:
            return any(
                row.id == job.id
                for row in db.peek_import_preview_job_candidates(
                    limit=10, execution_lease=execution_lease,
                )
            )

        def import_offers(execution_lease: ExecutionLeaseSnapshot | None) -> bool:
            return any(
                row.id == job.id
                for row in db.peek_import_job_candidates(
                    limit=10, execution_lease=execution_lease,
                )
            )

        # Must still work first, so the refusal is a refusal.
        self.assertTrue(preview_offers(lease))
        self.assertFalse(preview_offers(None))

        # The import lane reaches this row only through the real preview
        # claim: an automation job cannot be marked importable without one.
        with db.advisory_lock(ADVISORY_LOCK_NAMESPACE_IMPORT, 4406) as held:
            self.assertTrue(held)
            self.assertIsNotNone(db.claim_automation_import_preview_job_under_lock(
                job.id, request_id=4406, worker_id="preview",
                execution_lease=lease,
            ))
        self.assertIsNotNone(db.mark_import_job_preview_importable(
            job.id, expected_execution_lease=lease,
        ))
        self.assertTrue(import_offers(lease))
        self.assertFalse(import_offers(None))

    def test_a_live_beets_child_refuses_every_candidate_type(self) -> None:
        """Production's scan-level refusal, which the fake used to skip.

        ``peek_import_job_candidates`` and its preview twin both return
        ``[]`` outright while the caller's lease holds a Beets child. The
        fake folded that rule into the automation arm alone, so a force,
        local or YouTube row stayed on offer mid-Beets-mutation — the same
        fake-more-permissive shape as the guards above, in the two scans
        #1313 rewrote.
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        automation = self._owner(db, 4407)
        db.seed_request(make_request_row(
            id=4408, mb_release_id="fake-lane-beets", status="wanted",
        ))
        force = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=4408,
            payload=force_import_payload(
                download_log_id=4408, failed_path="/failed/beets",
            ),
        )

        def offered(execution_lease: ExecutionLeaseSnapshot | None) -> set[int]:
            return {
                row.id
                for row in db.peek_import_preview_job_candidates(
                    limit=10, execution_lease=execution_lease,
                )
            }

        # Must still work: a free lease offers both types.
        self.assertEqual(offered(self._lease()), {automation.id, force.id})
        # A live Beets child takes the force row away too, not just the
        # automation one its own arm names.
        self.assertEqual(offered(self._lease(beets=True)), set())


class TestFakeMergeRekeyForceClaimFence(unittest.TestCase):
    """The fake's force arm, term for term against the real SQL (#1080).

    ``update_request_release_for_merge``'s force arm is a five-term
    conjunction: a ``force_import`` job, ``running``, naming THIS request, on
    a row with no automation owner whose status is neither ``processing`` nor
    ``replaced``. Two permissiveness mutants — dropping ``job.status ==
    "running"`` and dropping ``job.request_id == request_id`` — survived the
    whole relevant suite before this table existed, while their real-SQL twins
    were killed by ``tests/test_pipeline_db.py::TestMergeRekeyUnderAForceClaim``
    on real PostgreSQL. A fake more permissive than the write it stands in for
    is exactly the test-fidelity Rule B failure: every seam test above it
    would agree with a production write that refuses.

    Every term is exercised on its own, from a world that otherwise rekeys.
    The one exception says so where it sits: the automation-owned case flips
    both owner terms at once, because migration 066's CHECK means PostgreSQL
    can only ever present them together. The ``processing`` status on its own
    is a fake-only world, and has its own case.
    """

    MERGED = "merged-id"
    SURVIVOR = "survivor-id"

    def _force_job(
        self,
        db: FakePipelineDB,
        *,
        request_id: int,
        download_log_id: int,
        claim: bool = True,
    ) -> int:
        from lib.import_queue import (
            IMPORT_JOB_FORCE,
            force_import_dedupe_key,
            force_import_payload,
        )

        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=request_id,
            dedupe_key=force_import_dedupe_key(download_log_id),
            payload=force_import_payload(
                download_log_id=download_log_id,
                failed_path="/quarantine/album",
            ),
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="ready",
        )
        if claim:
            claimed = db.claim_force_import_job_under_lock(
                job.id, request_id=request_id, worker_id="fence-test",
            )
            assert claimed is not None and claimed.status == "running"
        return job.id

    def _world(self, *, status: str = "wanted", claim: bool = True):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, mb_release_id=self.MERGED, status=status,
        ))
        job_id = self._force_job(
            db, request_id=41, download_log_id=1, claim=claim,
        )
        return db, job_id

    def _local_job(
        self, db: FakePipelineDB, *, request_id: int, claim: bool = True,
    ) -> int:
        """Sibling of ``_force_job`` (issue #1176 PR3): the fake's
        ``force_claim`` merge-rekey predicate was widened to admit
        ``local_import`` too — see ``lib.pipeline_db.requests
        .PipelineDB.rekey_release_identity``'s docstring."""
        from lib.import_queue import (
            IMPORT_JOB_LOCAL,
            local_import_dedupe_key,
            local_import_payload,
        )

        job = db.enqueue_import_job(
            IMPORT_JOB_LOCAL,
            request_id=request_id,
            dedupe_key=local_import_dedupe_key(request_id),
            payload=local_import_payload(
                source_path="/operator/album", request_id=request_id,
            ),
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="ready",
        )
        if claim:
            claimed = db.claim_local_import_job_under_lock(
                job.id, request_id=request_id, worker_id="fence-test",
            )
            assert claimed is not None and claimed.status == "running"
        return job.id

    def test_a_claimed_running_local_import_job_rekeys_its_own_request(self):
        """Mirrors the FORCE test above exactly, proving the widened
        ``force_claim`` predicate covers local_import too."""
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41, mb_release_id=self.MERGED, status="wanted",
        ))
        job_id = self._local_job(db, request_id=41)

        self.assertTrue(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])

    def _rekey(self, db: FakePipelineDB, job_id: int, request_id: int = 41):
        return db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=job_id,
        )

    def test_a_claimed_running_force_job_rekeys_its_own_request(self):
        db, job_id = self._world()

        self.assertTrue(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        # The lifecycle is untouched: force borrows the identity, never owns
        # the request.
        self.assertEqual(row["status"], "wanted")
        self.assertIsNone(row["active_automation_import_job_id"])

    def test_every_runnable_status_is_a_legal_force_rekey_target(self):
        for status in ("wanted", "imported", "unsearchable", "downloading"):
            with self.subTest(status=status):
                db, job_id = self._world(status=status)

                self.assertTrue(self._rekey(db, job_id))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_a_job_that_is_not_running_writes_nothing(self):
        """``queued`` is not a claim, and neither is a finished job."""
        db, job_id = self._world(claim=False)
        job = db.get_import_job(job_id)
        assert job is not None and job.status != "running"

        self.assertFalse(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

        db, job_id = self._world()
        self.assertIsNotNone(db.mark_import_job_completed(
            job_id, result={}, message="done",
        ))

        self.assertFalse(self._rekey(db, job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_job_naming_another_request_writes_nothing(self):
        db, _ = self._world()
        db.seed_request(make_request_row(id=42, mb_release_id="other-id"))
        other_job_id = self._force_job(db, request_id=42, download_log_id=2)

        # The claimed job is real and running — it just does not name request
        # 41, which is the term the mutant dropped.
        running = db.get_import_job(other_job_id)
        assert running is not None
        self.assertEqual(running.status, "running")
        self.assertFalse(self._rekey(db, other_job_id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_non_force_job_writes_nothing(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db, _ = self._world()
        youtube = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )

        self.assertFalse(self._rekey(db, youtube.id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_an_automation_owned_row_writes_nothing_under_a_force_claim(self):
        """Both owner terms at once — the world PostgreSQL can actually hold.

        Migration 066's owner-equivalence CHECK ties them together, so this
        is deliberately a two-term case. The ``processing`` term on its own is
        exercised by the next test, which the CHECK makes unreachable in
        PostgreSQL but perfectly reachable in the fake.
        """
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "processing"
        row["active_automation_import_job_id"] = 777

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)

    def test_a_processing_row_with_no_owner_writes_nothing(self):
        """The ``processing`` term alone, which only the fake can hold.

        The real SQL states ``status <> 'processing'`` and the owner-is-NULL
        term separately, and migration 066's CHECK means PostgreSQL can never
        present the first without the second — so in real PG the owner term
        already refuses this world and the status term is unreachable. The
        fake has no CHECK: it is the only place the status term can be
        exercised on its own, and every seam test in this repository runs
        against the fake. Without this case a fake force arm that admits
        ``processing`` agrees with a production write that refuses.
        """
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "processing"
        self.assertIsNone(row["active_automation_import_job_id"])

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)
        self.assertEqual(after["status"], "processing")

    def test_a_replaced_row_writes_nothing_under_a_force_claim(self):
        """Frozen audit ancestors are out of scope for BOTH claims."""
        db, job_id = self._world()
        row = db.request(41)
        assert row is not None
        row["status"] = "replaced"

        self.assertFalse(self._rekey(db, job_id))

        after = db.request(41)
        assert after is not None
        self.assertEqual(after["mb_release_id"], self.MERGED)

    def test_a_stale_identity_writes_nothing_under_a_force_claim(self):
        db, job_id = self._world()

        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="somebody-elses-id",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=job_id,
        ))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)


class TestFakeMergeRekeyOperatorClaimFence(unittest.TestCase):
    """The fake's operator arm, term for term against the real SQL (#1089).

    ``update_request_release_for_merge``'s operator arm is a four-term
    conjunction: ``expected_import_job_id IS NULL``, ``status = 'imported'``,
    no automation owner attached, and no ``queued``/``running`` import job at
    all for this request (any job type — unlike the force arm's own
    ``EXISTS``, this ``NOT EXISTS`` carries no ``job_type`` filter). Every
    term is exercised on its own from a world that otherwise rekeys, mirroring
    ``TestFakeMergeRekeyForceClaimFence`` above: a fake more permissive than
    the write it stands in for is the test-fidelity Rule B failure this class
    exists to prevent.
    """

    MERGED = "merged-id"
    SURVIVOR = "survivor-id"

    def _world(self, *, status: str = "imported", owner: int | None = None):
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=41,
            mb_release_id=self.MERGED,
            status=status,
            active_automation_import_job_id=owner,
        ))
        return db

    def _rekey(
        self,
        db: FakePipelineDB,
        *,
        request_id: int = 41,
        expected_import_job_id: int | None = None,
    ):
        return db.update_request_release_for_merge(
            request_id,
            old_release_id=self.MERGED,
            new_release_id=self.SURVIVOR,
            expected_import_job_id=expected_import_job_id,
        )

    def test_an_operator_call_rekeys_an_imported_unowned_unclaimed_row(self):
        db = self._world()

        self.assertTrue(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)
        self.assertEqual(row["status"], "imported")
        self.assertIsNone(row["active_automation_import_job_id"])
        self.assertEqual(
            db.update_request_release_for_merge_calls,
            [(41, self.MERGED, self.SURVIVOR, None)],
        )

    def test_a_real_job_id_never_satisfies_the_operator_arm(self):
        """``expected_import_job_id IS NULL`` — the arm-widening guard.

        A world that is otherwise exactly the operator's own (imported,
        unowned, nothing active) must still refuse a caller that supplies a
        real job id, even a job with no bearing on either claim arm (queued,
        not force). Dropping this guard would let the operator arm silently
        widen a force/automation caller's own — narrower — claim fence.
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = self._world()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=41,
            dedupe_key="force-41",
            payload=force_import_payload(
                download_log_id=1, failed_path="/quarantine/album",
            ),
        )
        db.mark_import_job_completed(job.id, result={}, message="done")

        self.assertFalse(self._rekey(db, expected_import_job_id=job.id))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_non_imported_status_writes_nothing_under_the_operator_arm(self):
        for status in (
            "wanted", "downloading", "unsearchable", "processing", "replaced",
        ):
            with self.subTest(status=status):
                db = self._world(status=status)

                self.assertFalse(self._rekey(db))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_an_automation_owned_imported_row_writes_nothing(self):
        """The owner term alone — reachable on ``imported`` only in the fake.

        Migration 066 ties the owner pointer to ``processing`` in real
        PostgreSQL, so this exact combination (``imported`` + an owner) never
        occurs there — but the fake has no CHECK, and it stands in for every
        seam test in this repository, so the term is exercised directly.
        """
        db = self._world(owner=777)

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_queued_import_job_blocks_the_operator_arm(self):
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db = self._world()
        db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_running_import_job_blocks_the_operator_arm(self):
        """No ``job_type`` filter — an in-flight rescue blocks it too."""
        from lib.import_queue import IMPORT_JOB_YOUTUBE, youtube_import_payload

        db = self._world()
        job = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=41,
            payload=youtube_import_payload(
                staged_path="/Incoming/auto-import/album",
                request_id=41,
                browse_id="MPREb_x",
                download_log_id=9,
            ),
        )
        db.mark_import_job_preview_importable(
            job.id, preview_result={}, message="ready",
        )
        claimed = db.claim_import_job_candidate(job.id, worker_id="fence-test")
        assert claimed is not None and claimed.status == "running"

        self.assertFalse(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)

    def test_a_terminal_import_job_never_blocks_the_operator_arm(self):
        """Must-still-work: a completed/failed job on this request is inert."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        for outcome in ("completed", "failed"):
            with self.subTest(outcome=outcome):
                db = self._world()
                job = db.enqueue_import_job(
                    IMPORT_JOB_FORCE,
                    request_id=41,
                    dedupe_key=f"force-41-{outcome}",
                    payload=force_import_payload(
                        download_log_id=1, failed_path="/quarantine/album",
                    ),
                )
                if outcome == "completed":
                    db.mark_import_job_completed(job.id, result={}, message="done")
                else:
                    db.mark_import_job_failed(job.id, error="synthetic failure")

                self.assertTrue(self._rekey(db))

                row = db.request(41)
                assert row is not None
                self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_an_active_job_on_a_different_request_never_blocks(self):
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = self._world()
        db.seed_request(make_request_row(id=42, mb_release_id="other-id"))
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force-42",
            payload=force_import_payload(
                download_log_id=2, failed_path="/quarantine/other",
            ),
        )

        self.assertTrue(self._rekey(db))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.SURVIVOR)

    def test_a_stale_identity_writes_nothing_under_the_operator_arm(self):
        db = self._world()

        self.assertFalse(db.update_request_release_for_merge(
            41,
            old_release_id="somebody-elses-id",
            new_release_id=self.SURVIVOR,
            expected_import_job_id=None,
        ))

        row = db.request(41)
        assert row is not None
        self.assertEqual(row["mb_release_id"], self.MERGED)


class TestFakeImportJobStubLifecycle(unittest.TestCase):
    """The import-job queue stubs mirror the real claim/complete lifecycle
    on both the automation and preview lanes.
    """

    def test_import_job_queue_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        first = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        duplicate = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertEqual(first.id, duplicate.id)
        self.assertTrue(duplicate.deduped)
        self.assertEqual(db.count_import_jobs_by_status(), {"queued": 1})
        db.mark_import_job_preview_importable(
            first.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )

        claimed = claim_next_import_job(db, worker_id="fake-worker")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 1)
        self.assertEqual(claimed.worker_id, "fake-worker")

        requeued = db.recover_running_import_jobs(
            requeue_message="retry",
            recovery_message="recovery required",
        )
        self.assertEqual([job.id for job in requeued], [claimed.id])
        self.assertEqual(requeued[0].status, "queued")
        self.assertIsNone(requeued[0].worker_id)

        claimed = claim_next_import_job(db, worker_id="fake-worker-2")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        self.assertEqual(claimed.attempts, 2)
        self.assertEqual(claimed.worker_id, "fake-worker-2")

        completed = db.mark_import_job_completed(
            claimed.id,
            result={"success": True},
            message="done",
        )
        assert completed is not None
        self.assertEqual(completed.status, "completed")

        later = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:42",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertNotEqual(first.id, later.id)
        failed = db.mark_import_job_failed(
            later.id,
            error="boom",
            message="failed",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")

    def test_import_job_queue_defaults_to_preview_waiting(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        queued = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:fresh",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )

        self.assertEqual(queued.preview_status, "waiting")
        self.assertIsNone(queued.preview_message)
        self.assertIsNone(queued.preview_completed_at)
        self.assertIsNone(queued.importable_at)
        # Preview worker can claim it; importer cannot.
        self.assertIsNone(claim_next_import_job(db, worker_id="importer"))
        claimed = claim_next_import_preview_job(db, worker_id="preview")
        assert claimed is not None
        self.assertEqual(claimed.id, queued.id)

    def test_import_job_preview_methods_mirror_core_lifecycle(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        db.seed_request(make_request_row(id=43, status="wanted"))
        queued = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:preview",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        self.assertEqual(queued.preview_status, "waiting")

        claimed = claim_next_import_preview_job(db, worker_id="fake-preview")
        assert claimed is not None
        self.assertEqual(claimed.status, "queued")
        self.assertEqual(claimed.preview_status, "running")
        self.assertEqual(claimed.preview_attempts, 1)
        self.assertEqual(claimed.preview_worker_id, "fake-preview")
        self.assertTrue(db.heartbeat_import_job_preview(claimed.id))

        importable = db.mark_import_job_preview_importable(
            claimed.id,
            preview_result={"verdict": "would_import"},
            message="Preview would import",
        )
        assert importable is not None
        self.assertEqual(importable.preview_status, "evidence_ready")
        self.assertEqual(importable.preview_result, {"verdict": "would_import"})
        self.assertIsNotNone(importable.importable_at)

        rejected = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=43,
            dedupe_key="force:preview-reject",
            payload={"download_log_id": 1, "failed_path": "/tmp/reject"},
        )
        failed = db.mark_import_job_preview_failed(
            rejected.id,
            preview_status="confident_reject",
            error="spectral_reject",
            preview_result={
                "verdict": "confident_reject",
                "reason": "spectral_reject",
            },
            message="Preview rejected: spectral_reject",
        )
        assert failed is not None
        self.assertEqual(failed.status, "failed")
        self.assertEqual(failed.preview_status, "confident_reject")
        self.assertEqual(failed.preview_error, "spectral_reject")
        self.assertEqual(failed.error, "spectral_reject")

    def test_requeue_import_job_for_preview_flips_running_back_to_waiting(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=42, status="wanted"))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:requeue-fake",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            message="ready",
        )
        claimed = claim_next_import_job(db, worker_id="importer")
        assert claimed is not None
        self.assertEqual(claimed.status, "running")
        prior_attempts = claimed.attempts
        prior_preview_attempts = claimed.preview_attempts

        updated = db.requeue_import_job_for_preview(
            claimed.id,
            reason="candidate evidence missing",
        )

        assert updated is not None
        self.assertEqual(updated.status, "queued")
        self.assertEqual(updated.preview_status, "waiting")
        self.assertIsNone(updated.worker_id)
        self.assertIsNone(updated.started_at)
        self.assertIsNone(updated.heartbeat_at)
        self.assertIsNone(updated.preview_message)
        self.assertIsNone(updated.preview_error)
        self.assertEqual(updated.message, "candidate evidence missing")
        # Counters preserved.
        self.assertEqual(updated.attempts, prior_attempts)
        self.assertEqual(updated.preview_attempts, prior_preview_attempts)

        # Candidate selection owns the requeue delay.
        self.assertIsNone(claim_next_import_preview_job(
            db, worker_id="preview-too-soon"))
        row = next(row for row in db._import_jobs if row["id"] == claimed.id)
        row["updated_at"] -= timedelta(seconds=61)
        preview = claim_next_import_preview_job(db, worker_id="preview-1")
        assert preview is not None
        self.assertEqual(preview.id, claimed.id)

    def test_requeue_import_job_for_preview_idempotent_when_not_running(self):
        from lib.import_queue import IMPORT_JOB_FORCE

        db = FakePipelineDB()
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=42,
            dedupe_key="force:requeue-fake-idem",
            payload={"download_log_id": 1, "failed_path": "/tmp/force"},
        )
        # Not yet claimed by importer (preview_status='waiting', status='queued').
        result = db.requeue_import_job_for_preview(
            job.id,
            reason="not running",
        )
        self.assertIsNone(result)

    def test_automation_commands_require_exact_owner_stage_and_lease(self):
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ProcessIdentity,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=42,
            mb_release_id="fake-owner-lease",
            status="wanted",
        ))
        job = handoff_automation_owner(
            db,
            42,
            canonical_path="/processing/albums/fake-owner-lease",
        )
        preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="preview-a",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=101, start_ticks=1001),
        )
        stale_preview_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="preview-stale",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=102, start_ticks=1002),
        )

        self.assertIsNone(claim_next_import_preview_job(db, worker_id="no-lease"))
        claimed_preview = claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=preview_lease,)
        assert claimed_preview is not None
        self.assertEqual(
            claimed_preview.execution_invocation_id,
            preview_lease.invocation_id,
        )
        self.assertEqual(db.requeue_stale_import_preview_jobs(
            older_than=timedelta(seconds=-1),
            message="heartbeat age is not automation proof",
        ), [])
        self.assertEqual(db.requeue_running_import_preview_jobs(
            message="process restart is not automation proof",
        ), [])
        self.assertFalse(db.heartbeat_import_job_preview(
            job.id,
            expected_execution_lease=stale_preview_lease,
        ))
        self.assertFalse(db.set_import_job_candidate_evidence(
            job.id,
            77,
            expected_execution_lease=stale_preview_lease,
        ))
        self.assertTrue(db.set_import_job_candidate_evidence(
            job.id,
            77,
            expected_execution_lease=preview_lease,
        ))
        self.assertIsNotNone(db.mark_import_job_preview_importable(
            job.id,
            preview_result={"verdict": "would_import"},
            expected_execution_lease=preview_lease,
        ))

        importer_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="importer-a",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(pid=201, start_ticks=2001),
        )
        claimed_import = claim_next_import_job(db, worker_id="importer",
        execution_lease=importer_lease,)
        assert claimed_import is not None
        self.assertFalse(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=preview_lease,
        ))
        self.assertTrue(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=importer_lease,
        ))

        # A wrong stage/status cannot borrow even the exact execution lease.
        db._requests[42]["status"] = "wanted"
        self.assertFalse(db.heartbeat_import_job(
            job.id,
            expected_execution_lease=importer_lease,
        ))
        db._requests[42]["status"] = "processing"

    def test_automation_startup_recovery_requires_exact_dead_proof(self):
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ExecutionLivenessDecision,
            ExecutionLivenessEvidence,
            ProcessIdentity,
        )

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=44,
            mb_release_id="fake-startup-recovery",
            status="wanted",
        ))
        job = handoff_automation_owner(db, 44)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="preview-old",
            systemd_unit="cratedigger-import-preview.service",
            worker=ProcessIdentity(pid=501, start_ticks=5001),
        )
        assert claim_next_import_preview_job(db, worker_id="preview",
        execution_lease=lease,) is not None

        exact_evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-new",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        )
        live = ExecutionLivenessDecision(
            status="live",
            reason="still alive",
            evidence=exact_evidence,
        )
        self.assertIsNone(db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=live,
            requeue_message="requeue",
            recovery_message="operator recovery",
        ))

        stale_lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-old",
            invocation_id="preview-other",
            systemd_unit=lease.systemd_unit,
            worker=lease.worker,
        )
        stale_dead = ExecutionLivenessDecision(
            status="dead",
            reason="different invocation ended",
            evidence=ExecutionLivenessEvidence(
                lease=stale_lease,
                current_host_boot_id="boot-new",
                boot_error=None,
                worker=None,
                beets=None,
                invocation=None,
                cgroup=None,
            ),
        )
        self.assertIsNone(db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=stale_dead,
            requeue_message="requeue",
            recovery_message="operator recovery",
        ))

        dead = ExecutionLivenessDecision(
            status="dead",
            reason="prior boot ended",
            evidence=exact_evidence,
        )
        recovered = db.recover_automation_import_job(
            job.id,
            expected_execution_lease=lease,
            decision=dead,
            requeue_message="requeue",
            recovery_message="operator recovery",
        )
        assert recovered is not None
        self.assertEqual(recovered.status, "queued")
        self.assertEqual(recovered.preview_status, "waiting")
        self.assertIsNone(recovered.execution_invocation_id)


class TestFakeTerminalForceWrongMatchCleanupJobs(unittest.TestCase):
    """``list_terminal_force_wrong_match_cleanup_jobs`` mirrors a SQL
    predicate with several independent arms, so it gets its own class.
    """

    def test_list_terminal_force_wrong_match_cleanup_jobs_mirrors_sql_predicate(
        self,
    ) -> None:
        """Issue #1122: the fake must select the same rows the real SQL does.

        Real-PG proof of the same predicate lives in
        ``tests/test_pipeline_db.py`` — this pins the fake against an
        IDENTICAL scenario matrix so the two never silently drift
        (test-fidelity.md's fake-vs-SQL predicate drift class). Covers the
        review-round corrections: MAJOR-1 (success-keyed, not
        presence-keyed), MAJOR-2/3 (the era-AND-lane marker excludes every
        historical/non-adjudicating shape by construction).
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        db.seed_request(make_request_row(id=1))

        def _force_job(suffix: str):
            return db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=1,
                dedupe_key=f"force-wrong-match-predicate:{suffix}",
                payload=force_import_payload(
                    download_log_id=1,
                    failed_path="/tmp/predicate-source",
                ),
            )

        # -- completed arm --------------------------------------------------

        completed_missing = _force_job("completed-missing")
        db.mark_import_job_completed(
            completed_missing.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
            },
            message="done",
        )

        completed_failed_receipt = _force_job("completed-failed-receipt")
        db.mark_import_job_completed(
            completed_failed_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {
                    "success": False, "error": "path_unavailable: EACCES",
                },
            },
            message="done",
        )

        completed_successful_receipt = _force_job("completed-successful-receipt")
        db.mark_import_job_completed(
            completed_successful_receipt.id,
            result={
                "success": True, "message": "done", "deferred": False,
                "code": None, "post_commit_wrong_match_scenario": None,
                "wrong_match_dismissal": {"success": True},
            },
            message="done",
        )

        # -- failed arm -------------------------------------------------

        failed_missing = _force_job("failed-missing")
        db.mark_import_job_failed(
            failed_missing.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
            },
            message="rejected",
        )

        failed_failed_receipt = _force_job("failed-failed-receipt")
        db.mark_import_job_failed(
            failed_failed_receipt.id,
            error="beets rejected: audio_corrupt",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "audio_corrupt",
                "cleanup": {
                    "success": False, "outcome": "deleted_operator_force_source",
                    "error": "path_unavailable: EACCES",
                },
            },
            message="rejected",
        )

        failed_successful_receipt = _force_job("failed-successful-receipt")
        db.mark_import_job_failed(
            failed_successful_receipt.id,
            error="beets rejected",
            result={
                "success": False, "message": "rejected", "deferred": False,
                "code": None,
                "post_commit_wrong_match_scenario": "high_distance",
                "cleanup": {
                    "success": True,
                    "outcome": "preserved_operator_force_source",
                },
            },
            message="rejected",
        )

        failed_requeue = _force_job("failed-requeue")
        db.mark_import_job_failed(
            failed_requeue.id,
            error="requeue failed",
            result={
                "success": False, "message": "requeue UPDATE failed",
                "deferred": False, "code": "requeue_failed",
                "post_commit_wrong_match_scenario": None,
            },
            message="requeue UPDATE failed",
        )

        failed_requeue_exhausted = _force_job("failed-requeue-exhausted")
        db.mark_import_job_failed(
            failed_requeue_exhausted.id,
            error="preview/import requeue budget exhausted",
            result={
                "success": False, "message": "budget exhausted",
                "deferred": False, "code": "requeue_exhausted",
                "post_commit_wrong_match_scenario": None,
            },
            message="budget exhausted",
        )

        failed_deferred = _force_job("failed-deferred")
        db.mark_import_job_failed(
            failed_deferred.id,
            error="Another import is already in progress",
            result={
                "success": False,
                "message": "Another import is already in progress",
                "deferred": True, "code": None,
                "post_commit_wrong_match_scenario": None,
            },
            message="Another import is already in progress",
        )

        # -- historical / non-adjudicating shapes (MAJOR-2/3) ------------

        historical_completed = _force_job("historical-completed-no-marker")
        db.mark_import_job_completed(
            historical_completed.id,
            result={"success": True},
            message="done",
        )

        historical_failed = _force_job("historical-failed-no-marker")
        db.mark_import_job_failed(
            historical_failed.id,
            error="RuntimeError: boom",
            result={"success": False},
            message="Executor crashed",
        )

        # A genuinely NULL ``result`` column has no public-API constructor
        # on the fake either (``mark_import_job_failed`` always writes
        # ``result or {}``) — reach into the fake's own row store directly,
        # mirroring the real-PG test's raw ``UPDATE ... result = NULL``.
        historical_null_result = _force_job("historical-null-result")
        db.mark_import_job_failed(historical_null_result.id, error="boom")
        for row in db._import_jobs:
            if row["id"] == historical_null_result.id:
                row["result"] = None
                break

        selected = {
            job.id
            for job in db.list_terminal_force_wrong_match_cleanup_jobs()
        }
        self.assertIn(completed_missing.id, selected)
        self.assertIn(completed_failed_receipt.id, selected)
        self.assertNotIn(completed_successful_receipt.id, selected)
        self.assertIn(failed_missing.id, selected)
        self.assertIn(failed_failed_receipt.id, selected)
        self.assertNotIn(failed_successful_receipt.id, selected)
        self.assertNotIn(failed_requeue.id, selected)
        self.assertNotIn(failed_requeue_exhausted.id, selected)
        self.assertNotIn(failed_deferred.id, selected)
        self.assertNotIn(historical_completed.id, selected)
        self.assertNotIn(historical_failed.id, selected)
        self.assertNotIn(historical_null_result.id, selected)


class TestFakeBeetsChildRefusalIsNotAutomationOnly(unittest.TestCase):
    """A live Beets child refuses every job type, not just automation.

    Four production methods spell that rule as an unconditional early
    return above their SQL, so for a force, local or YouTube job the
    Python guard is the whole enforcement: none of those statements'
    non-automation arms reads ``execution_beets_pid`` at all. The fake
    folded each one into its automation arm, where a non-automation row
    short-circuits past it.

    This is the shape #1313 already fixed at the two candidate scans
    (``test_a_live_beets_child_refuses_every_candidate_type``); it did not
    reach these four. Measured on the 2026-09-02 #1347 pass by driving the
    real ``PipelineDB`` and the fake through the same helper against an
    ephemeral PostgreSQL: production wrote nothing, the fake advanced the
    row.
    """

    def _lease(self, beets: bool) -> ExecutionLeaseSnapshot:
        return ExecutionLeaseSnapshot(
            host_boot_id="boot-beets-child",
            invocation_id="invocation-beets-child",
            systemd_unit="cratedigger-import-preview-worker.service",
            worker=ProcessIdentity(pid=711, start_ticks=7011),
            beets=ProcessIdentity(pid=712, start_ticks=7012) if beets else None,
        )

    def _previewing_youtube_job(self, db: FakePipelineDB, request_id: int):
        """A non-automation row parked at ``queued``/``running``."""
        from lib.import_queue import IMPORT_JOB_YOUTUBE

        db.seed_request(make_request_row(
            id=request_id,
            mb_release_id=f"fake-beets-child-{request_id}",
            status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_YOUTUBE,
            request_id=request_id,
            payload={
                "staged_path": "/incoming/album",
                "request_id": request_id,
                "browse_id": "MPREb_beets_child",
                "download_log_id": 1,
            },
        )
        claimed = db.claim_import_preview_job_candidate(
            job.id, worker_id="preview",
        )
        assert claimed is not None
        return claimed

    def test_a_live_beets_child_refuses_a_non_automation_preview_heartbeat(
        self,
    ) -> None:
        db = FakePipelineDB()
        job = self._previewing_youtube_job(db, 4501)

        # Must still work: a lease with no child refreshes the stamp, so
        # the refusal below is a refusal and not a dead fixture.
        self.assertTrue(db.heartbeat_import_job_preview(
            job.id, expected_execution_lease=self._lease(beets=False),
        ))
        before = db.get_import_job(job.id)
        assert before is not None

        self.assertFalse(db.heartbeat_import_job_preview(
            job.id, expected_execution_lease=self._lease(beets=True),
        ))
        after = db.get_import_job(job.id)
        assert after is not None
        self.assertEqual(
            after.preview_heartbeat_at, before.preview_heartbeat_at,
        )

    def test_a_live_beets_child_refuses_a_non_automation_importable_mark(
        self,
    ) -> None:
        db = FakePipelineDB()
        job = self._previewing_youtube_job(db, 4502)

        self.assertIsNone(db.mark_import_job_preview_importable(
            job.id, expected_execution_lease=self._lease(beets=True),
        ))
        blocked = db.get_import_job(job.id)
        assert blocked is not None
        self.assertEqual(blocked.preview_status, "running")

        # Must still work: the same call without a child hands the row on.
        self.assertIsNotNone(db.mark_import_job_preview_importable(
            job.id, expected_execution_lease=self._lease(beets=False),
        ))
        handed_on = db.get_import_job(job.id)
        assert handed_on is not None
        self.assertEqual(handed_on.preview_status, "evidence_ready")

    def test_a_live_beets_child_refuses_a_non_automation_launch(self) -> None:
        """The Beets launch fence — the highest-stakes member of the four."""
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload
        from tests.evidence_helpers import make_album_quality_evidence

        db = FakePipelineDB()
        source_path = "/failed/beets-child-launch"
        db.seed_request(make_request_row(
            id=4503, mb_release_id="release-beets-child", status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=4503,
            payload=force_import_payload(
                download_log_id=4503, failed_path=source_path,
            ),
        )
        evidence = make_album_quality_evidence(
            mb_release_id="release-beets-child", source_path=source_path,
        )
        db.upsert_album_quality_evidence(evidence)
        persisted = db.find_album_quality_evidence(
            mb_release_id="release-beets-child",
            snapshot_fingerprint=evidence.snapshot_fingerprint,
        )
        assert persisted is not None and persisted.id is not None
        db.set_import_job_candidate_evidence(job.id, persisted.id)
        assert db.mark_import_job_preview_importable(job.id) is not None
        claimed = claim_next_import_job(db, worker_id="importer")
        assert claimed is not None

        self.assertIsNone(db.authorize_import_job_launch(
            claimed.id,
            request_id=4503,
            release_id="release-beets-child",
            source_path=source_path,
            expected_execution_lease=self._lease(beets=True),
        ))
        refused = db.get_import_job(claimed.id)
        assert refused is not None
        self.assertIsNone(refused.beets_launch_authorized_at)

        # Must still work: no child, and the same call authorizes.
        self.assertIsNotNone(db.authorize_import_job_launch(
            claimed.id,
            request_id=4503,
            release_id="release-beets-child",
            source_path=source_path,
            expected_execution_lease=self._lease(beets=False),
        ))
        authorized = db.get_import_job(claimed.id)
        assert authorized is not None
        self.assertIsNotNone(authorized.beets_launch_authorized_at)


class TestFakeAutomationHandoffRowShape(unittest.TestCase):
    """The handoff INSERT's column list, which the fake used to flatten.

    ``handoff_automation_import``'s INSERT names seven columns and omits
    ``preview_message``, ``preview_completed_at`` and ``importable_at``, so
    migrations 005/018's DEFAULTs fire and a fresh automation job is born
    carrying ``'Preview gate disabled'`` and two ``NOW()`` stamps. The
    other two writers into this table (``enqueue_import_job`` and
    ``enqueue_youtube_import_and_mark_success``) name the same three and
    write explicit NULLs, so every non-automation job is born with NULLs.
    The fake ran all of them through one ``_append_import_job`` that
    hard-coded NULL, erasing the difference.

    Live evidence is ``importable_at``, not ``preview_message``. The
    message DEFAULT is transient: ``mark_import_job_preview_importable``
    overwrites ``preview_message`` with the preview lane's own text, so no
    settled row still shows it. ``importable_at`` survives because that
    same writer only ``COALESCE``s it. Of the rows created since
    2026-07-31 (measured 2026-09-02, the settled cohort after
    ``handoff_automation_import`` shipped on 2026-07-29), all 1,350
    ``automation_import`` rows carry ``importable_at = created_at`` and
    not one of the 92 force/youtube/local rows does.

    Do not cite the 7,558 rows whose ``preview_message`` still reads
    ``'Preview gate disabled'`` as evidence for this: every one was
    created between 2026-04-25 and 2026-05-12, before
    ``handoff_automation_import`` existed, and all 7,558 carry
    ``preview_status = 'would_import'`` where the handoff explicitly
    writes ``'waiting'``. They come from the pre-#898 ``enqueue_import_job``,
    which omitted ``preview_status`` too. The #1347 review round caught
    that misattribution.

    The asymmetry is production's to keep or change, not the fake's to
    smooth over: ``importable_at`` is the candidate scan's sort key
    (``ORDER BY importable_at ASC NULLS LAST``), so an automation job
    sorts by its handoff time while a force job sorts by its
    preview-completion time. Flattening it in the fake let a test pin a
    queue order production does not produce.
    """

    def test_handoff_leaves_the_three_defaulted_columns_to_the_database(
        self,
    ) -> None:
        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4504, mb_release_id="fake-handoff-shape", status="wanted",
        ))
        job = handoff_automation_owner(db, 4504)

        self.assertEqual(job.preview_status, "waiting")
        self.assertEqual(job.preview_message, "Preview gate disabled")
        self.assertIsNotNone(job.preview_completed_at)
        self.assertIsNotNone(job.importable_at)

    def test_enqueue_still_writes_those_three_columns_as_null(self) -> None:
        """The other writer, so the pin above is a difference, not a default.

        ``test_import_job_queue_defaults_to_preview_waiting`` asserts the
        same four values; this one exists for the contrast, and reads as
        half of a pair with the handoff pin above rather than on its own.
        """
        from lib.import_queue import IMPORT_JOB_FORCE, force_import_payload

        db = FakePipelineDB()
        db.seed_request(make_request_row(
            id=4505, mb_release_id="fake-enqueue-shape", status="wanted",
        ))
        job = db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=4505,
            payload=force_import_payload(
                download_log_id=4505, failed_path="/failed/enqueue-shape",
            ),
        )

        self.assertEqual(job.preview_status, "waiting")
        self.assertIsNone(job.preview_message)
        self.assertIsNone(job.preview_completed_at)
        self.assertIsNone(job.importable_at)


if __name__ == "__main__":
    unittest.main()
