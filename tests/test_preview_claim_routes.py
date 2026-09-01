"""Pins for the preview worker's per-job-type claim-route registry.

The preview lane's twin of ``tests/test_importer_job_kinds.py``: the importer
replaced its if/elif claim ladder with ``_IMPORT_JOB_KINDS`` in issue #1278,
and the preview worker kept the ladder until issue #1313's lane series. These
pins hold the registry to the routing the ladder performed, including the two
arms that quietly differed from each other.
"""

from __future__ import annotations

import unittest
from collections.abc import Callable

from lib.import_execution import ExecutionLeaseSnapshot, ProcessIdentity
from lib.import_queue import (
    IMPORT_JOB_AUTOMATION,
    IMPORT_JOB_FORCE,
    IMPORT_JOB_LOCAL,
    IMPORT_JOB_TYPES,
    IMPORT_JOB_YOUTUBE,
    ImportJob,
)
from lib.import_worker_loop import ClaimState
from scripts import import_preview_worker
from scripts.import_preview_worker import _PreviewHeartbeatDB


class _StubHeartbeatDB:
    """Satisfies ``_PreviewHeartbeatDB`` without opening a connection."""

    def heartbeat_import_job_preview(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot | None = None,
    ) -> bool:
        del job_id, expected_execution_lease
        return True

    def close(self) -> None:
        return None

_LEASE = ExecutionLeaseSnapshot(
    host_boot_id="boot-preview-routes",
    invocation_id="invocation-preview-routes",
    systemd_unit="cratedigger-import-preview-worker.service",
    worker=ProcessIdentity(pid=31, start_ticks=3100),
)


def _job(job_type: str, job_id: int = 1) -> ImportJob:
    payload: dict[str, object] = {
        IMPORT_JOB_FORCE: {"download_log_id": 1, "failed_path": "/q/album"},
        IMPORT_JOB_LOCAL: {"source_path": "/op/album", "request_id": 5},
        IMPORT_JOB_AUTOMATION: {},
        IMPORT_JOB_YOUTUBE: {
            "staged_path": "/incoming/album",
            "request_id": 5,
            "browse_id": "MPREb_x",
            "download_log_id": 1,
        },
    }[job_type]
    return ImportJob.from_row({
        "id": job_id,
        "job_type": job_type,
        "status": "queued",
        "request_id": 5,
        "dedupe_key": None,
        "payload": payload,
        "result": None,
        "message": None,
        "error": None,
        "attempts": 0,
        "worker_id": None,
        "created_at": None,
        "updated_at": None,
        "started_at": None,
        "heartbeat_at": None,
        "completed_at": None,
    })


class _RecordingClaimer:
    """A queue-connection stand-in for the unguarded (plain) route."""

    def __init__(self, *, dsn: str | None = None) -> None:
        self.claimed: list[tuple[int, str]] = []
        if dsn is not None:
            self.dsn = dsn

    def peek_import_preview_job_candidates(
        self,
        *,
        execution_lease: ExecutionLeaseSnapshot | None = None,
        limit: int,
        offset: int = 0,
    ) -> list[ImportJob]:
        del execution_lease, limit, offset
        return []

    def claim_import_preview_job_candidate(
        self,
        job_id: int,
        *,
        worker_id: str,
    ) -> ImportJob | None:
        self.claimed.append((job_id, worker_id))
        return _job(IMPORT_JOB_YOUTUBE, job_id)


def _noop_process(*_args: object, **_kwargs: object) -> ImportJob | None:
    return None


def _attempt(
    db: _RecordingClaimer,
    *,
    execution_lease: ExecutionLeaseSnapshot | None = None,
    process_fn: Callable[..., ImportJob | None] = _noop_process,
    heartbeat_db_factory: Callable[[str], _PreviewHeartbeatDB] | None = None,
) -> import_preview_worker._PreviewClaimAttempt:
    return import_preview_worker._PreviewClaimAttempt(
        db=db,
        worker_id="preview-worker",
        execution_lease=execution_lease,
        heartbeat_interval=1.0,
        runtime_config=None,
        stage_db_factory=lambda _dsn: object(),
        heartbeat_db_factory=heartbeat_db_factory or (
            lambda _dsn: _StubHeartbeatDB()
        ),
        raw_heartbeat_db_factory=heartbeat_db_factory,
        candidate_measurement_fn=None,
        process_fn=process_fn,
        claim_state=ClaimState(),
    )


class TestPreviewClaimRouteRegistry(unittest.TestCase):
    def test_registry_covers_exactly_the_declared_job_types(self) -> None:
        """A new job type must gain a route, not fall through a default."""
        self.assertEqual(
            set(import_preview_worker._PREVIEW_CLAIM_ROUTES),
            set(IMPORT_JOB_TYPES),
        )

    def test_each_job_type_names_its_own_route(self) -> None:
        expected = {
            IMPORT_JOB_AUTOMATION: import_preview_worker._preview_route_automation,
            IMPORT_JOB_FORCE: import_preview_worker._preview_route_force_import,
            IMPORT_JOB_LOCAL: import_preview_worker._preview_route_local_import,
            IMPORT_JOB_YOUTUBE: import_preview_worker._preview_route_plain,
        }
        for job_type, route in expected.items():
            with self.subTest(job_type=job_type):
                self.assertIs(
                    import_preview_worker._preview_claim_route_for(job_type),
                    route,
                )

    def test_an_unrouted_job_type_falls_back_to_the_unguarded_route(
        self,
    ) -> None:
        """Exactly what the retired ladder's ``else`` arm did."""
        self.assertIs(
            import_preview_worker._preview_claim_route_for("not_a_job_type"),
            import_preview_worker._preview_route_plain,
        )

    def test_force_and_local_are_distinct_routes(self) -> None:
        self.assertIsNot(
            import_preview_worker._preview_route_force_import,
            import_preview_worker._preview_route_local_import,
        )


class TestAutomationRouteRefusals(unittest.TestCase):
    """Both ``continue`` arms the ladder used, now as unmarked claims."""

    def test_no_execution_lease_leaves_the_claim_unmarked(self) -> None:
        attempt = _attempt(_RecordingClaimer(dsn="dsn"), execution_lease=None)
        self.assertIsNone(
            import_preview_worker._preview_route_automation(
                _job(IMPORT_JOB_AUTOMATION), attempt,
            ),
        )
        self.assertFalse(attempt.claim_state.claimed)

    def test_no_dsn_leaves_the_claim_unmarked(self) -> None:
        attempt = _attempt(_RecordingClaimer(), execution_lease=_LEASE)
        self.assertIsNone(
            import_preview_worker._preview_route_automation(
                _job(IMPORT_JOB_AUTOMATION), attempt,
            ),
        )
        self.assertFalse(attempt.claim_state.claimed)


class TestRequestScopedRouteRefusals(unittest.TestCase):
    def test_no_dsn_leaves_the_claim_unmarked(self) -> None:
        for job_type, route in (
            (IMPORT_JOB_FORCE, import_preview_worker._preview_route_force_import),
            (IMPORT_JOB_LOCAL, import_preview_worker._preview_route_local_import),
        ):
            with self.subTest(job_type=job_type):
                attempt = _attempt(_RecordingClaimer())
                self.assertIsNone(route(_job(job_type), attempt))
                self.assertFalse(attempt.claim_state.claimed)


class TestPlainRoute(unittest.TestCase):
    def test_a_claim_marks_the_state_and_runs_the_processor(self) -> None:
        db = _RecordingClaimer()
        seen: list[tuple[object, int]] = []

        def _process(
            db_arg: object, job: ImportJob, **_kwargs: object,
        ) -> ImportJob | None:
            seen.append((db_arg, job.id))
            return None

        attempt = _attempt(db, process_fn=_process)
        import_preview_worker._preview_route_plain(
            _job(IMPORT_JOB_YOUTUBE, 42), attempt,
        )
        self.assertTrue(attempt.claim_state.claimed)
        self.assertEqual(db.claimed, [(42, "preview-worker")])
        self.assertEqual(len(seen), 1)
        self.assertIs(seen[0][0], db)
        self.assertEqual(seen[0][1], 42)

    def test_it_forwards_the_callers_own_heartbeat_factory_including_none(
        self,
    ) -> None:
        """The one place the two claim shapes genuinely disagree.

        The pinned-session routes need a concrete heartbeat factory and get
        ``heartbeat_db_factory or PipelineDB``; this route hands the caller's
        own value straight to ``process_fn``, which reads ``None`` as "use my
        own default". Substituting ``PipelineDB`` here would silently open a
        real connection in every test that passes ``None``.
        """
        forwarded: list[object] = []

        def _process(
            _db: object, _job: ImportJob, **kwargs: object,
        ) -> ImportJob | None:
            forwarded.append(kwargs["heartbeat_db_factory"])
            return None

        attempt = _attempt(
            _RecordingClaimer(), process_fn=_process, heartbeat_db_factory=None,
        )
        import_preview_worker._preview_route_plain(
            _job(IMPORT_JOB_YOUTUBE), attempt,
        )
        self.assertEqual(forwarded, [None])

    def test_a_refused_claim_leaves_the_state_unmarked(self) -> None:
        class _Refusing(_RecordingClaimer):
            def claim_import_preview_job_candidate(
                self,
                job_id: int,
                *,
                worker_id: str,
            ) -> ImportJob | None:
                del job_id, worker_id
                return None

        attempt = _attempt(_Refusing())
        self.assertIsNone(
            import_preview_worker._preview_route_plain(
                _job(IMPORT_JOB_YOUTUBE), attempt,
            ),
        )
        self.assertFalse(attempt.claim_state.claimed)


if __name__ == "__main__":
    unittest.main()
