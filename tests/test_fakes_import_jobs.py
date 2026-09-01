"""Self-tests for the FakePipelineDB import-job lane cluster.

Split out of ``tests/test_fakes.py`` (#1313) so each cluster of the
fake has its sibling tests beside it.
"""
import unittest

from tests.dispatch_helpers import (
    claim_next_import_job,
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
        from lib.import_execution import (
            ExecutionLeaseSnapshot,
            ProcessIdentity,
        )

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
        drove it: replacing the whole force/local arm with ``return True``
        left tests.test_import_queue, tests.test_importer_job_kinds,
        tests.test_fakes, tests.test_fakes_import_jobs,
        tests.test_force_import_gates, tests.test_local_import_lane,
        tests.test_import_operation_fence and
        tests.test_import_job_lane_generated all green.
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


if __name__ == "__main__":
    unittest.main()
