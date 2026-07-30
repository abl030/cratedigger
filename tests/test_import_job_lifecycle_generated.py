"""Generated runnable-import-job lifecycle boundary for issue #663."""

from __future__ import annotations

import unittest
from contextlib import contextmanager

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.dispatch import DispatchOutcome
from lib.import_execution import (
    CancellationToken,
    ExecutionLeaseSnapshot,
    OwnerSessionIdentity,
)
from lib.import_queue import (
    IMPORT_JOB_FORCE,
    IMPORT_JOB_PREVIEW_EVIDENCE_READY,
    IMPORT_JOB_PREVIEW_STATUSES,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
    youtube_import_payload,
)
from lib.pipeline_db import ADVISORY_LOCK_NAMESPACE_IMPORT
from tests.fakes import FakePipelineDB
from tests.helpers import claim_next_import_job, make_request_row


def assert_only_evidence_ready_is_claimable(
    preview_status: str,
    claimed: bool,
) -> None:
    """The importer lane begins only after neutral persisted evidence."""
    expected = preview_status == IMPORT_JOB_PREVIEW_EVIDENCE_READY
    if claimed != expected:
        raise AssertionError(
            f"preview_status={preview_status!r} claimed={claimed}; "
            f"expected {expected}",
        )


def _claimed_for(preview_status: str) -> bool:
    db = FakePipelineDB()
    db.seed_request(make_request_row(id=663, status="wanted"))
    db.enqueue_import_job(
        IMPORT_JOB_FORCE,
        request_id=663,
        dedupe_key=f"issue-663:{preview_status}",
        payload={"download_log_id": 663, "failed_path": "/tmp/663"},
    )
    db._import_jobs[0]["preview_status"] = preview_status
    return claim_next_import_job(db, worker_id="generated") is not None


def _unavailable_execution_lease(
    **_kwargs: object,
) -> ExecutionLeaseSnapshot:
    raise ValueError("generated run is outside systemd")


def assert_unrelated_candidate_progressed(
    *,
    expected_request_id: int,
    observed_request_ids: list[int],
) -> None:
    if observed_request_ids != [expected_request_id]:
        raise AssertionError(
            "bounded scan failed to progress the first unrelated candidate: "
            f"expected={[expected_request_id]} observed={observed_request_ids}"
        )


def assert_all_older_candidates_progressed(
    *,
    older_job_ids: list[int],
    observed_job_ids: list[int],
) -> None:
    missing = [
        job_id for job_id in older_job_ids
        if job_id not in observed_job_ids
    ]
    if missing:
        raise AssertionError(
            "sustained-growth scan stranded older candidates: "
            f"missing={missing} observed={observed_job_ids}"
        )


class TestImportJobRunnableLifecycleGenerated(unittest.TestCase):
    def test_checker_rejects_the_removed_would_import_compatibility(self) -> None:
        with self.assertRaisesRegex(AssertionError, "would_import"):
            assert_only_evidence_ready_is_claimable("would_import", True)

    def test_only_evidence_ready_preview_status_is_claimable(self) -> None:
        # Exhaustive finite status vocabulary, including the removed
        # would_import compatibility and the evidence_ready success world.
        for preview_status in sorted(IMPORT_JOB_PREVIEW_STATUSES):
            with self.subTest(preview_status=preview_status):
                assert_only_evidence_ready_is_claimable(
                    preview_status,
                    _claimed_for(preview_status),
                )

    @given(
        lane=st.sampled_from(("import", "preview")),
        blocked_count=st.integers(min_value=32, max_value=40),
    )
    @example(lane="import", blocked_count=32)
    @example(lane="preview", blocked_count=32)
    def test_rotating_bounded_scan_progresses_beyond_duplicate_request_window(
        self,
        lane: str,
        blocked_count: int,
    ) -> None:
        from scripts import import_preview_worker, importer

        first_request_id = 7_000
        first_free_request_id = first_request_id + 1
        blocked = {first_request_id}
        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        for request_id in (first_request_id, first_free_request_id):
            db.seed_request(make_request_row(
                id=request_id,
                status="wanted",
            ))
        for offset in range(blocked_count + 1):
            request_id = (
                first_request_id
                if offset < blocked_count
                else first_free_request_id
            )
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"generated-scan:{lane}:{offset}",
                payload={
                    "download_log_id": request_id,
                    "failed_path": f"/tmp/generated-scan-{offset}",
                },
            )
            if lane == "import":
                db._import_jobs[-1]["preview_status"] = (
                    IMPORT_JOB_PREVIEW_EVIDENCE_READY
                )

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                with db._pin_owner_session(token) as identity:
                    yield identity

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if (
                    namespace == ADVISORY_LOCK_NAMESPACE_IMPORT
                    and key in blocked
                ):
                    yield False
                else:
                    with db.advisory_lock(namespace, key) as acquired:
                        yield acquired

            def claim_force_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def claim_force_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_preview_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def close(self) -> None:
                return None

        observed: list[int] = []

        def execute_import(
            _stage_db: object,
            claimed: ImportJob,
            *,
            ctx: object | None = None,
            cancellation_token: CancellationToken,
            owner_session_identity: OwnerSessionIdentity,
        ) -> DispatchOutcome:
            del ctx, cancellation_token, owner_session_identity
            assert claimed.request_id is not None
            observed.append(claimed.request_id)
            return DispatchOutcome(False, "generated progress")

        def execute_preview(
            _stage_db: object,
            claimed: ImportJob,
            **_kwargs: object,
        ) -> ImportJob:
            assert claimed.request_id is not None
            observed.append(claimed.request_id)
            return claimed

        import_cursor = importer._CandidateScanCursor()
        if lane == "import":
            for _poll in range(2):
                importer.run_once(
                    db,  # pyright: ignore[reportArgumentType]
                    worker_id="generated-import-scan",
                    stage_db_factory=lambda _dsn: StageSession(),
                    execution_lease_factory=_unavailable_execution_lease,
                    execute_fn=execute_import,
                    scan_cursor=import_cursor,
                )
        else:
            preview_cursor = import_preview_worker._CandidateScanCursor()
            for _poll in range(2):
                import_preview_worker.run_once(
                    db,
                    worker_id="generated-preview-scan",
                    stage_db_factory=lambda _dsn: StageSession(),
                    execution_lease_factory=_unavailable_execution_lease,
                    process_fn=execute_preview,
                    scan_cursor=preview_cursor,
                )

        assert_unrelated_candidate_progressed(
            expected_request_id=first_free_request_id,
            observed_request_ids=observed,
        )

    def test_checker_rejects_restart_at_prefix_starvation_mutant(self) -> None:
        """A worker that resets offset zero each poll never observes row 33."""
        from scripts import importer

        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        for request_id in (7_000, 7_001):
            db.seed_request(make_request_row(id=request_id, status="wanted"))
        for offset in range(33):
            request_id = 7_000 if offset < 32 else 7_001
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=request_id,
                dedupe_key=f"reset-prefix-mutant:{offset}",
                payload={
                    "download_log_id": request_id,
                    "failed_path": f"/tmp/reset-prefix-mutant-{offset}",
                },
            )
            db._import_jobs[-1]["preview_status"] = (
                IMPORT_JOB_PREVIEW_EVIDENCE_READY
            )

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                with db._pin_owner_session(token) as identity:
                    yield identity

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if (
                    namespace == ADVISORY_LOCK_NAMESPACE_IMPORT
                    and key == 7_000
                ):
                    yield False
                else:
                    with db.advisory_lock(namespace, key) as acquired:
                        yield acquired

            def claim_force_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def close(self) -> None:
                return None

        observed_from_two_reset_polls: list[int] = []

        def execute_import(
            _stage_db: object,
            claimed: ImportJob,
            **_kwargs: object,
        ) -> DispatchOutcome:
            assert claimed.request_id is not None
            observed_from_two_reset_polls.append(claimed.request_id)
            return DispatchOutcome(False, "known-bad should not reach row 33")

        # Known-bad implementation: recreating the cursor on every call.
        for _poll in range(2):
            importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="reset-prefix-mutant",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=_unavailable_execution_lease,
                execute_fn=execute_import,
            )

        with self.assertRaisesRegex(AssertionError, "bounded scan failed"):
            assert_unrelated_candidate_progressed(
                expected_request_id=7_001,
                observed_request_ids=observed_from_two_reset_polls,
            )

    @given(lane=st.sampled_from(("import", "preview")))
    @example(lane="import")
    @example(lane="preview")
    def test_rotating_scan_wraps_after_population_shrink(
        self,
        lane: str,
    ) -> None:
        """An exhausted offset wraps to a candidate added after shrink."""
        from scripts import import_preview_worker, importer

        blocked_request_id = 8_000
        new_request_id = 8_001
        db = FakePipelineDB()
        setattr(db, "dsn", "postgresql://fake")  # noqa: B010
        for request_id in (blocked_request_id, new_request_id):
            db.seed_request(make_request_row(id=request_id, status="wanted"))
        for offset in range(32):
            db.enqueue_import_job(
                IMPORT_JOB_FORCE,
                request_id=blocked_request_id,
                dedupe_key=f"wrap-shrink:{lane}:old:{offset}",
                payload={
                    "download_log_id": blocked_request_id,
                    "failed_path": f"/tmp/wrap-shrink-old-{offset}",
                },
            )
            if lane == "import":
                db._import_jobs[-1]["preview_status"] = (
                    IMPORT_JOB_PREVIEW_EVIDENCE_READY
                )

        class StageSession:
            def __getattr__(self, name: str) -> object:
                return getattr(db, name)

            @contextmanager
            def _pin_owner_session(self, token: CancellationToken):
                with db._pin_owner_session(token) as identity:
                    yield identity

            @contextmanager
            def advisory_lock(self, namespace: int, key: int):
                if (
                    namespace == ADVISORY_LOCK_NAMESPACE_IMPORT
                    and key == blocked_request_id
                ):
                    yield False
                else:
                    with db.advisory_lock(namespace, key) as acquired:
                        yield acquired

            def claim_force_import_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def claim_force_import_preview_job_under_lock(
                self,
                job_id: int,
                *,
                request_id: int,
                worker_id: str | None,
            ) -> ImportJob | None:
                return db.claim_force_import_preview_job_under_lock(
                    job_id,
                    request_id=request_id,
                    worker_id=worker_id,
                )

            def close(self) -> None:
                return None

        observed: list[int] = []

        def execute_import(
            _stage_db: object,
            claimed: ImportJob,
            **_kwargs: object,
        ) -> DispatchOutcome:
            assert claimed.request_id is not None
            observed.append(claimed.request_id)
            return DispatchOutcome(False, "wrapped import candidate")

        def execute_preview(
            _stage_db: object,
            claimed: ImportJob,
            **_kwargs: object,
        ) -> ImportJob:
            assert claimed.request_id is not None
            observed.append(claimed.request_id)
            return claimed

        import_cursor = importer._CandidateScanCursor()
        preview_cursor = import_preview_worker._CandidateScanCursor()
        if lane == "import":
            importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="wrap-shrink-import",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=_unavailable_execution_lease,
                execute_fn=execute_import,
                scan_cursor=import_cursor,
            )
            cursor_offset = import_cursor.offset
        else:
            import_preview_worker.run_once(
                db,
                worker_id="wrap-shrink-preview",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=_unavailable_execution_lease,
                process_fn=execute_preview,
                scan_cursor=preview_cursor,
            )
            cursor_offset = preview_cursor.offset
        self.assertEqual(cursor_offset, 32)
        self.assertEqual(observed, [])

        # All rows in the exhausted page disappear, then a new row arrives at
        # offset zero. The missing-reset mutant performs only this first query.
        db._import_jobs.clear()
        db.enqueue_import_job(
            IMPORT_JOB_FORCE,
            request_id=new_request_id,
            dedupe_key=f"wrap-shrink:{lane}:new",
            payload={
                "download_log_id": new_request_id,
                "failed_path": "/tmp/wrap-shrink-new",
            },
        )
        if lane == "import":
            db._import_jobs[-1]["preview_status"] = (
                IMPORT_JOB_PREVIEW_EVIDENCE_READY
            )
            mutant_page = db.peek_import_job_candidates(
                limit=importer.IMPORT_CANDIDATE_SCAN_LIMIT,
                offset=import_cursor.offset,
            )
        else:
            mutant_page = db.peek_import_preview_job_candidates(
                limit=import_preview_worker.PREVIEW_CANDIDATE_SCAN_LIMIT,
                offset=preview_cursor.offset,
            )
        self.assertEqual(mutant_page, [])
        with self.assertRaisesRegex(AssertionError, "bounded scan failed"):
            assert_unrelated_candidate_progressed(
                expected_request_id=new_request_id,
                observed_request_ids=[],
            )

        if lane == "import":
            importer.run_once(
                db,  # pyright: ignore[reportArgumentType]
                worker_id="wrap-shrink-import",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=_unavailable_execution_lease,
                execute_fn=execute_import,
                scan_cursor=import_cursor,
            )
        else:
            import_preview_worker.run_once(
                db,
                worker_id="wrap-shrink-preview",
                stage_db_factory=lambda _dsn: StageSession(),
                execution_lease_factory=_unavailable_execution_lease,
                process_fn=execute_preview,
                scan_cursor=preview_cursor,
            )

        assert_unrelated_candidate_progressed(
            expected_request_id=new_request_id,
            observed_request_ids=observed,
        )

    @given(
        lane=st.sampled_from(("import", "preview")),
        older_count=st.integers(min_value=4, max_value=12),
        arrivals_per_poll=st.integers(min_value=2, max_value=4),
    )
    @example(lane="import", older_count=4, arrivals_per_poll=2)
    @example(lane="preview", older_count=4, arrivals_per_poll=2)
    def test_successful_claim_resets_for_bounded_revisit_under_growth(
        self,
        lane: str,
        older_count: int,
        arrivals_per_poll: int,
    ) -> None:
        """Every success revisits older work despite steady arrivals."""
        from scripts import import_preview_worker, importer

        def run_world(*, nonzero_offset_mutant: bool) -> tuple[list[int], list[int]]:
            db = FakePipelineDB()
            sequence = 0

            def enqueue() -> int:
                nonlocal sequence
                sequence += 1
                request_id = 9_000 + sequence
                db.seed_request(make_request_row(
                    id=request_id,
                    status="wanted",
                ))
                job = db.enqueue_import_job(
                    IMPORT_JOB_YOUTUBE,
                    request_id=request_id,
                    dedupe_key=f"sustained-growth:{lane}:{sequence}",
                    payload=youtube_import_payload(
                        staged_path=f"/tmp/sustained-growth-{sequence}",
                        request_id=request_id,
                        browse_id=f"browse-{sequence}",
                        download_log_id=sequence,
                    ),
                )
                if lane == "import":
                    db._import_jobs[-1]["preview_status"] = (
                        IMPORT_JOB_PREVIEW_EVIDENCE_READY
                    )
                return job.id

            older_job_ids = [enqueue() for _item in range(older_count)]
            observed_job_ids: list[int] = []

            def execute_import(
                _db: object,
                claimed: ImportJob,
                **_kwargs: object,
            ) -> DispatchOutcome:
                observed_job_ids.append(claimed.id)
                return DispatchOutcome(False, "generated sustained growth")

            def execute_preview(
                _db: object,
                claimed: ImportJob,
                **_kwargs: object,
            ) -> ImportJob:
                observed_job_ids.append(claimed.id)
                return claimed

            import_cursor = importer._CandidateScanCursor()
            preview_cursor = import_preview_worker._CandidateScanCursor()
            for _poll in range(older_count):
                if lane == "import":
                    importer.run_once(
                        db,  # pyright: ignore[reportArgumentType]
                        worker_id="sustained-growth-import",
                        execution_lease_factory=_unavailable_execution_lease,
                        execute_fn=execute_import,
                        scan_cursor=import_cursor,
                    )
                    if nonzero_offset_mutant:
                        import_cursor.offset += 1
                else:
                    import_preview_worker.run_once(
                        db,
                        worker_id="sustained-growth-preview",
                        execution_lease_factory=_unavailable_execution_lease,
                        process_fn=execute_preview,
                        scan_cursor=preview_cursor,
                    )
                    if nonzero_offset_mutant:
                        preview_cursor.offset += 1
                for _arrival in range(arrivals_per_poll):
                    enqueue()

            return older_job_ids, observed_job_ids

        older_job_ids, observed_job_ids = run_world(
            nonzero_offset_mutant=False,
        )
        assert_all_older_candidates_progressed(
            older_job_ids=older_job_ids,
            observed_job_ids=observed_job_ids,
        )

        mutant_older, mutant_observed = run_world(
            nonzero_offset_mutant=True,
        )
        with self.assertRaisesRegex(AssertionError, "stranded older"):
            assert_all_older_candidates_progressed(
                older_job_ids=mutant_older,
                observed_job_ids=mutant_observed,
            )

    @given(lane=st.sampled_from(("import", "preview")))
    @example(lane="import")
    @example(lane="preview")
    def test_success_revisits_released_prefix_with_replenished_tail(
        self,
        lane: str,
    ) -> None:
        """Cleared contention progresses even while the tail stays nonempty."""
        from scripts import import_preview_worker, importer

        def run_world(
            *,
            retain_tail_offset_mutant: bool,
        ) -> tuple[list[int], list[int]]:
            inner = FakePipelineDB()
            sequence = 0

            def enqueue() -> int:
                nonlocal sequence
                sequence += 1
                request_id = 10_000 + sequence
                inner.seed_request(make_request_row(
                    id=request_id,
                    status="wanted",
                ))
                job = inner.enqueue_import_job(
                    IMPORT_JOB_YOUTUBE,
                    request_id=request_id,
                    dedupe_key=f"released-prefix:{lane}:{sequence}",
                    payload=youtube_import_payload(
                        staged_path=f"/tmp/released-prefix-{sequence}",
                        request_id=request_id,
                        browse_id=f"released-prefix-{sequence}",
                        download_log_id=sequence,
                    ),
                )
                if lane == "import":
                    inner._import_jobs[-1]["preview_status"] = (
                        IMPORT_JOB_PREVIEW_EVIDENCE_READY
                    )
                return job.id

            older_job_ids = [enqueue() for _item in range(32)]
            enqueue()  # First tail row, just beyond the bounded page.
            contended = True

            class ContendedDB:
                def __getattr__(self, name: str) -> object:
                    return getattr(inner, name)

                def peek_import_preview_job_candidates(
                    self,
                    *,
                    execution_lease: ExecutionLeaseSnapshot | None = None,
                    limit: int,
                    offset: int = 0,
                ) -> list[ImportJob]:
                    return inner.peek_import_preview_job_candidates(
                        execution_lease=execution_lease,
                        limit=limit,
                        offset=offset,
                    )

                def claim_import_job_candidate(
                    self,
                    job_id: int,
                    *,
                    worker_id: str,
                ) -> ImportJob | None:
                    if contended and job_id in older_job_ids:
                        return None
                    return inner.claim_import_job_candidate(
                        job_id,
                        worker_id=worker_id,
                    )

                def claim_import_preview_job_candidate(
                    self,
                    job_id: int,
                    *,
                    worker_id: str,
                ) -> ImportJob | None:
                    if contended and job_id in older_job_ids:
                        return None
                    return inner.claim_import_preview_job_candidate(
                        job_id,
                        worker_id=worker_id,
                    )

            db = ContendedDB()
            observed_job_ids: list[int] = []

            def execute_import(
                _db: object,
                claimed: ImportJob,
                **_kwargs: object,
            ) -> DispatchOutcome:
                observed_job_ids.append(claimed.id)
                return DispatchOutcome(False, "released-prefix import")

            def execute_preview(
                _db: object,
                claimed: ImportJob,
                **_kwargs: object,
            ) -> ImportJob:
                observed_job_ids.append(claimed.id)
                return claimed

            import_cursor = importer._CandidateScanCursor()
            preview_cursor = import_preview_worker._CandidateScanCursor()

            # One fully contended page advances to the tail.
            if lane == "import":
                importer.run_once(
                    db,  # pyright: ignore[reportArgumentType]
                    worker_id="released-prefix-import",
                    execution_lease_factory=_unavailable_execution_lease,
                    execute_fn=execute_import,
                    scan_cursor=import_cursor,
                )
                cursor_offset = import_cursor.offset
            else:
                import_preview_worker.run_once(
                    db,
                    worker_id="released-prefix-preview",
                    execution_lease_factory=_unavailable_execution_lease,
                    process_fn=execute_preview,
                    scan_cursor=preview_cursor,
                )
                cursor_offset = preview_cursor.offset
            assert cursor_offset == 32
            assert observed_job_ids == []

            contended = False
            # Tail success plus one poll for every older row is the fixed
            # bound. A new tail row arrives after every successful claim.
            for _poll in range(33):
                before = len(observed_job_ids)
                if lane == "import":
                    importer.run_once(
                        db,  # pyright: ignore[reportArgumentType]
                        worker_id="released-prefix-import",
                        execution_lease_factory=_unavailable_execution_lease,
                        execute_fn=execute_import,
                        scan_cursor=import_cursor,
                    )
                    if retain_tail_offset_mutant:
                        import_cursor.offset = 32
                else:
                    import_preview_worker.run_once(
                        db,
                        worker_id="released-prefix-preview",
                        execution_lease_factory=_unavailable_execution_lease,
                        process_fn=execute_preview,
                        scan_cursor=preview_cursor,
                    )
                    if retain_tail_offset_mutant:
                        preview_cursor.offset = 32
                assert len(observed_job_ids) == before + 1
                enqueue()

            return older_job_ids, observed_job_ids

        older_job_ids, observed_job_ids = run_world(
            retain_tail_offset_mutant=False,
        )
        assert_all_older_candidates_progressed(
            older_job_ids=older_job_ids,
            observed_job_ids=observed_job_ids,
        )

        mutant_older, mutant_observed = run_world(
            retain_tail_offset_mutant=True,
        )
        with self.assertRaisesRegex(AssertionError, "stranded older"):
            assert_all_older_candidates_progressed(
                older_job_ids=mutant_older,
                observed_job_ids=mutant_observed,
            )


if __name__ == "__main__":
    unittest.main()
