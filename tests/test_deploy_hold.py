"""Deterministic contracts for the authoritative deployment hold."""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import unittest
from collections.abc import Mapping
from pathlib import Path
from unittest import mock

import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 - starts and migrates isolated PostgreSQL

import scripts.cratedigger_deploy_hold as deploy_hold_module
from scripts.cratedigger_deploy_hold import (
    _DRAIN_TIMEOUT_SECONDS,
    _PRODUCER_DRAIN_TIMEOUT_SECONDS,
    CONTROL_DIR,
    CONTROLLED_WORKER_UNITS,
    GATE_GUARDED_LINE,
    GATE_GUARDED_UNITS,
    GATE_RESUME_LINE,
    GATE_RESUME_UNITS,
    GATE_STOPPED_UNITS,
    MAIN_SERVICE,
    MAIN_TIMER,
    METADATA_MANUAL_HOLD,
    PHASE_ACQUIRING,
    PHASE_COMPLETE_PENDING,
    PHASE_HELD,
    PHASE_MAIN_TIMER_OPEN,
    PHASE_PREPARED_CONTROLLED,
    PRODUCER_SERVICE_UNITS,
    SERVICE_UNITS,
    START_INHIBITORS,
    TIMER_UNITS,
    UNFINDABLE_SERVICE,
    WATCHDOG_TIMER,
    YOUTUBE_SERVICE,
    DeployHoldBackend,
    DeployHoldError,
    JobState,
    LifecyclePreflight,
    RealSystemdBackend,
    UnitState,
    _assert_clean_old_lifecycle,
    _drain_services,
    _ensure_owned_control_mask,
    _ensure_owned_manual_hold,
    _ensure_owned_start_inhibitor,
    _wait_automation_queue_drained,
    abort_hold,
    acquire_hold,
    complete_release,
    finish_release,
    open_main_timer,
    prepare_controlled,
    recover_held,
    verify_held,
)
from scripts.pipeline_cli.query import _render_query_table
from tests._source_pins import pinned_source
from tests.fakes.deploy_hold import FakeDeployHoldBackend

INVOCATION = "a" * 32
REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_DSN = os.environ["TEST_DB_DSN"]


def _acquire_hold_pre_1078_order(backend: DeployHoldBackend) -> None:
    """Known-bad fixture: the exact pre-#1078 acquire order.

    The gate hold -- which stops every controlled worker, including the
    importer that is the only thing draining ``active_automation_jobs`` --
    is taken before that queue ever gets a chance to empty. Retained only to
    prove #1078's pins trip on it; never call this from production code.
    """
    backend.create_receipt()
    for timer in TIMER_UNITS:
        _ensure_owned_control_mask(backend, timer)
    backend.daemon_reload()
    backend.stop_units(TIMER_UNITS)
    _ensure_owned_manual_hold(backend)
    _drain_services(
        backend, SERVICE_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )
    _assert_clean_old_lifecycle(backend)
    backend.write_phase(PHASE_HELD)


def _drain_producers_then_hold_pre_must_fix_1(backend: DeployHoldBackend) -> None:
    """Known-bad fixture: #1078's own first-cut reorder, before MUST FIX 1.

    Drains ``PRODUCER_SERVICE_UNITS`` (including YouTube ingest) before
    taking the gate hold. YouTube ingest is ``Type=simple``,
    ``wantedBy=multi-user.target``, ``Restart=on-failure``, with no timer at
    all -- an always-on daemon nothing before the gate hold ever asks to
    stop -- so this hangs the full service-drain timeout and then fails with
    the gate hold never taken: the exact deadlock shape #1078 exists to
    remove, reproduced by the reorder's own first draft. Retained only to
    prove MUST FIX 1's pins trip on it; never call this from production code.
    """
    for timer in TIMER_UNITS:
        _ensure_owned_control_mask(backend, timer)
    backend.daemon_reload()
    backend.stop_units(TIMER_UNITS)
    _drain_services(
        backend, PRODUCER_SERVICE_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )
    _wait_automation_queue_drained(backend)
    _ensure_owned_manual_hold(backend)
    _drain_services(
        backend, GATE_STOPPED_UNITS,
        timeout_seconds=_DRAIN_TIMEOUT_SECONDS,
    )


class _PostgresLifecyclePreflightBackend(RealSystemdBackend):
    """Run production preflight SQL and preserve pipeline-cli table output."""

    def _pipeline_query(self, sql: str) -> str:
        with (
            psycopg2.connect(TEST_DSN) as conn,
            conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cursor,
        ):
            cursor.execute(sql)
            rows: list[Mapping[str, object]] = [
                {str(key): value for key, value in row.items()}
                for row in cursor.fetchall()
            ]
            columns = [
                description.name
                for description in (cursor.description or ())
            ]
        return "\n".join(_render_query_table(rows, columns))


class TestLifecyclePreflightPostgresContract(unittest.TestCase):
    """The strict hold's production SQL detects every dirty row shape."""

    conn: psycopg2.extensions.connection

    def setUp(self) -> None:
        self.conn = psycopg2.connect(TEST_DSN)
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                "TRUNCATE processing_cleanup_journal, import_jobs, "
                "album_requests CASCADE"
            )
        self.backend = _PostgresLifecyclePreflightBackend()
        self.request_sequence = 0

    def tearDown(self) -> None:
        self.conn.close()

    def _request(
        self,
        *,
        status: str,
        active_download_state: dict[str, object] | None = None,
    ) -> int:
        self.request_sequence += 1
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO album_requests (
                    mb_release_id,
                    artist_name,
                    album_title,
                    source,
                    status,
                    active_download_state
                )
                VALUES (%s, 'Artist', 'Album', 'request', %s, %s)
                RETURNING id
                """,
                (
                    f"deploy-hold-{self.request_sequence}",
                    status,
                    psycopg2.extras.Json(active_download_state)
                    if active_download_state is not None
                    else None,
                ),
            )
            row = cursor.fetchone()
            assert row is not None
            return int(row[0])

    def _automation_owner(self, *, status: str) -> tuple[int, int]:
        request_id = self._request(status="wanted")
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO import_jobs (
                    job_type, status, request_id, payload, preview_status
                )
                VALUES (
                    'automation_import', %s, %s, '{}'::jsonb, 'waiting'
                )
                RETURNING id
                """,
                (status, request_id),
            )
            row = cursor.fetchone()
            assert row is not None
            job_id = int(row[0])
            cursor.execute(
                """
                UPDATE album_requests
                SET status = 'processing',
                    active_automation_import_job_id = %s
                WHERE id = %s
                """,
                (job_id, request_id),
            )
        return request_id, job_id

    def test_counts_active_automation_and_recovery_jobs(self) -> None:
        self._automation_owner(status="queued")
        self._automation_owner(status="recovery_required")

        counts = self.backend.lifecycle_preflight()

        self.assertEqual(counts.active_automation_jobs, 2)
        self.assertEqual(counts.recovery_required_jobs, 1)
        self.assertEqual(counts.dirty_downloading_rows, 0)
        self.assertEqual(counts.malformed_enqueued_at_rows, 0)

    def test_counts_each_dirty_downloading_marker(self) -> None:
        markers: tuple[tuple[str, object], ...] = (
            ("processing_started_at", "2026-07-30T00:00:01+00:00"),
            ("current_path", "/processing/albums/exact-owner"),
            ("import_subprocess_started_at", "2026-07-30T00:00:02+00:00"),
        )
        for marker, value in markers:
            with self.subTest(marker=marker):
                with self.conn, self.conn.cursor() as cursor:
                    cursor.execute(
                        "TRUNCATE processing_cleanup_journal, import_jobs, "
                        "album_requests CASCADE"
                    )
                self._request(
                    status="downloading",
                    active_download_state={
                        "enqueued_at": "2026-07-30T00:00:00+00:00",
                        marker: value,
                    },
                )

                counts = self.backend.lifecycle_preflight()

                self.assertEqual(counts.dirty_downloading_rows, 1)
                self.assertEqual(counts.malformed_enqueued_at_rows, 0)

    def test_counts_each_malformed_enqueued_at_shape(self) -> None:
        malformed: tuple[object, ...] = (
            None,
            "",
            42,
            "not-a-timestamp",
        )
        for value in malformed:
            with self.subTest(value=value):
                with self.conn, self.conn.cursor() as cursor:
                    cursor.execute(
                        "TRUNCATE processing_cleanup_journal, import_jobs, "
                        "album_requests CASCADE"
                    )
                state: dict[str, object] = (
                    {} if value is None else {"enqueued_at": value}
                )
                self._request(
                    status="downloading",
                    active_download_state=state,
                )

                counts = self.backend.lifecycle_preflight()

                self.assertEqual(counts.dirty_downloading_rows, 0)
                self.assertEqual(counts.malformed_enqueued_at_rows, 1)

    def test_counts_sql_null_active_download_state_as_malformed(self) -> None:
        self._request(status="downloading", active_download_state=None)

        counts = self.backend.lifecycle_preflight()

        self.assertEqual(counts.dirty_downloading_rows, 0)
        self.assertEqual(counts.malformed_enqueued_at_rows, 1)

    def test_migration_066_enforcement_catalog_is_fully_installed(self) -> None:
        with self.conn, self.conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT (
                         SELECT count(*) = 1
                         FROM pg_index AS index_state
                         JOIN pg_class AS index_class
                           ON index_class.oid = index_state.indexrelid
                         JOIN pg_class AS indexed_table
                           ON indexed_table.oid = index_state.indrelid
                         WHERE index_class.relname
                                 = 'one_active_automation_import_per_request'
                           AND indexed_table.relname = 'import_jobs'
                           AND index_state.indisunique
                           AND index_state.indisvalid
                           AND index_state.indisready
                           AND index_state.indpred IS NOT NULL
                       ),
                       (
                         SELECT count(*) = 3
                         FROM pg_constraint
                         WHERE conname IN (
                           'album_requests_active_automation_owner_unique',
                           'album_requests_active_automation_owner_fk',
                           'processing_cleanup_journal_job_request_fk'
                         )
                           AND convalidated
                           AND condeferrable
                           AND condeferred
                       ),
                       (
                         SELECT count(*) = 3
                         FROM pg_trigger
                         WHERE tgname IN (
                           'album_requests_complete_processing_owner',
                           'import_jobs_complete_processing_owner',
                           'processing_cleanup_journal_exact_owner'
                         )
                           AND NOT tgisinternal
                           AND tgenabled = 'O'
                           AND tgdeferrable
                           AND tginitdeferred
                       )
                """
            )
            row = cursor.fetchone()

        self.assertEqual(row, (True, True, True))


class TestAcquireAuthoritativeHold(unittest.TestCase):
    def test_acquire_owns_exact_control_masks_and_manual_hold(self) -> None:
        backend = FakeDeployHoldBackend()

        acquire_hold(backend)

        backend.assert_default_held()
        self.assertEqual(
            backend.control_links,
            {timer: "/dev/null" for timer in TIMER_UNITS},
        )
        metadata_gate_index = backend.events.index(("metadata-gate", "hold manual"))
        for timer in TIMER_UNITS:
            link_index = backend.events.index(
                ("link-create", f"{CONTROL_DIR}/{timer}")
            )
            self.assertLess(link_index, metadata_gate_index)
        self.assertIn(("stop", *TIMER_UNITS), backend.events)
        self.assertNotIn(
            True,
            [
                event[0] == "link-create" and event[1].endswith(".service")
                for event in backend.events
            ],
        )
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).load_state, "masked")
        for service in SERVICE_UNITS:
            state = backend.unit_state(service)
            self.assertEqual((state.active_state, state.sub_state), ("inactive", "dead"))
            self.assertEqual(backend.job_state(service), JobState.none())

    def test_waiting_start_is_cancelled_but_running_oneshot_drains(self) -> None:
        waiting = JobState(
            job_id="41",
            unit=MAIN_SERVICE,
            job_type="start",
            state="waiting",
        )
        running_service = SERVICE_UNITS[1]
        running = JobState(
            job_id="42",
            unit=running_service,
            job_type="start",
            state="running",
        )
        backend = FakeDeployHoldBackend(
            jobs={MAIN_SERVICE: waiting, running_service: running},
            running_samples={running_service: 1},
        )

        acquire_hold(backend)

        self.assertEqual(backend.cancelled_jobs, ["41"])
        self.assertNotIn("42", backend.cancelled_jobs)
        self.assertEqual(backend.unit_state(running_service).active_state, "inactive")
        self.assertGreaterEqual(backend.sleep_calls, 2)

    def test_job_free_terminal_failure_is_reset_to_stable_inactivity(self) -> None:
        backend = FakeDeployHoldBackend(failed_services={MAIN_SERVICE})

        acquire_hold(backend)

        backend.assert_default_held()
        self.assertIn(("reset-failed", MAIN_SERVICE), backend.events)
        self.assertEqual(
            backend.unit_state(MAIN_SERVICE),
            UnitState("loaded", "inactive", "dead"),
        )

    def test_preexisting_manual_hold_fails_before_mutation(self) -> None:
        backend = FakeDeployHoldBackend(manual_hold=True)

        with self.assertRaisesRegex(DeployHoldError, "manual hold already exists"):
            acquire_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertEqual(backend.events, [])

    def test_preexisting_control_link_fails_before_mutation(self) -> None:
        backend = FakeDeployHoldBackend(
            control_links={MAIN_TIMER: "/dev/null"}
        )

        with self.assertRaisesRegex(DeployHoldError, "unowned control path"):
            acquire_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertTrue(backend.manual_hold is False)
        self.assertEqual(backend.events, [])

    def test_existing_acquiring_receipt_resumes_owned_intents(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.mark_link_owned(MAIN_TIMER)
        backend.mark_manual_hold_owned()

        acquire_hold(backend)

        backend.assert_default_held()
        self.assertEqual(
            backend.control_links,
            {timer: "/dev/null" for timer in TIMER_UNITS},
        )

    def test_interrupted_atomic_receipt_publication_can_retry(self) -> None:
        backend = FakeDeployHoldBackend(interrupt_receipt_publication=True)

        with self.assertRaisesRegex(InterruptedError, "publication interruption"):
            acquire_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertTrue(backend.staging_receipt)
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.control_links, {})
        acquire_hold(backend)
        backend.assert_default_held()
        self.assertFalse(backend.staging_receipt)

    def test_existing_non_acquiring_receipt_fails_closed(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.write_phase(PHASE_HELD)

        with self.assertRaisesRegex(DeployHoldError, "expected phase"):
            acquire_hold(backend)

    def test_stale_controlled_start_prerequisite_fails_before_mutation(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(
            controlled_start_contract_current=False,
        )

        with self.assertRaisesRegex(
            DeployHoldError,
            "controlled-start prerequisite changed",
        ):
            acquire_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertEqual(backend.events, [])

    def test_anomaly_lifecycle_field_stays_authoritatively_acquiring(self) -> None:
        """recovery_required_jobs/malformed_enqueued_at_rows drain nothing.

        Unlike active_automation_jobs/dirty_downloading_rows, nothing
        clears these -- so acquire must fail fast (not wait), and still
        leave the full boundary (masks + gate hold) established, since the
        failure comes from the final _assert_clean_old_lifecycle check, after
        the gate hold is already taken.
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(0, 1, 0, 0),
        )

        with self.assertRaisesRegex(
            DeployHoldError,
            "recovery_required_jobs",
        ):
            acquire_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        self.assertTrue(backend.manual_hold)
        self.assertEqual(backend.control_links, {
            timer: "/dev/null" for timer in TIMER_UNITS
        })
        self.assertEqual(backend.owned_inhibitor_units(), ())
        self.assertEqual(backend.inhibitor_files, set())

    def test_recovery_required_job_short_circuits_the_queue_wait(self) -> None:
        """#1078 MUST FIX 6.

        recovery_required_jobs is ALSO counted inside active_automation_jobs's
        own SQL (status IN ('queued', 'running', 'recovery_required')), so a
        stuck recovery-required job makes active_automation_jobs permanently
        nonzero too -- the naive wait would run the full 30-minute timeout
        and then report only the misleading aggregate ("queue" stuck, when
        the truth is a stuck anomaly). The wait must stop the moment an
        anomaly field is dirty and let the full, accurate field dict fail
        immediately instead: fast, correctly diagnosed, and still maximally
        quiesced (the gate hold is still taken before the failure).
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 1, 0, 0),
        )

        with self.assertRaises(DeployHoldError) as caught:
            acquire_hold(backend)

        message = str(caught.exception)
        self.assertIn("old lifecycle is not clean", message)
        self.assertIn("active_automation_jobs", message)
        self.assertIn("recovery_required_jobs", message)
        self.assertNotIn("timed out waiting", message)
        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        # The gate hold WAS taken -- maximally quiesced, not merely fast.
        self.assertTrue(backend.manual_hold)
        # Fast: the queue-drain wait never looped toward its own timeout.
        self.assertLess(backend.sleep_calls, 5)

    def test_acquire_times_out_waiting_for_a_queue_that_never_drains(self) -> None:
        """active_automation_jobs is drainable -- acquire waits, bounded.

        Unlike an anomaly field, this must NOT fail immediately at the final
        lifecycle check: it is a bounded wait for the still-running importer
        to empty the queue, and the gate hold is never taken while waiting.
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
        )

        # No timeout patch needed: the fake's clock advances by the real
        # requested duration per sleep() call and its sleep is instant, so
        # the unpatched production 1800s/5s-poll bound (360 loop iterations)
        # runs in this test at the exact bound production uses.
        with self.assertRaisesRegex(
            DeployHoldError,
            r"timed out waiting for the automation queue to drain: "
            r"active_automation_jobs=1 dirty_downloading_rows=0",
        ):
            acquire_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        # The whole point of the reorder: the gate hold was never taken.
        self.assertFalse(backend.manual_hold)
        self.assertFalse(backend.owned_manual_hold)
        self.assertEqual(backend.control_links, {
            timer: "/dev/null" for timer in TIMER_UNITS
        })
        # #1078 MUST FIX 5: the pre-hold window owns no start inhibitor at
        # all -- nothing is waited on for YouTube pre-hold, and masking
        # already blocks a fresh timer trigger (though not an unrelated
        # hold's resume-if-clear) -- so a reboot here leaves no persistent
        # /var/lib artifact to orphan.
        self.assertEqual(backend.owned_inhibitor_units(), ())
        self.assertEqual(backend.inhibitor_files, set())

    def test_acquire_completes_once_the_automation_queue_drains(self) -> None:
        """A job queued just before acquire arrives still lets it complete.

        The importer/preview default to active in this fake (the real
        world every acquire meets -- #1078 MUST FIX 7), which is what lets
        the queue-drain wait's latch observe them still running.
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
            queue_drain_after_calls=1,
        )

        acquire_hold(backend)

        backend.assert_default_held()
        self.assertEqual(backend.owned_inhibitor_units(), ())
        self.assertEqual(backend.inhibitor_files, set())
        preflight_indices = [
            index
            for index, event in enumerate(backend.events)
            if event == ("lifecycle-preflight",)
        ]
        # Two polls inside the queue-drain wait, one final proof afterward.
        self.assertEqual(len(preflight_indices), 3)
        gate_hold_index = backend.events.index(("metadata-gate", "hold manual"))
        self.assertTrue(all(index < gate_hold_index for index in preflight_indices[:2]))
        self.assertGreater(preflight_indices[2], gate_hold_index)

    def test_acquire_catches_main_started_in_the_window_right_after_the_hold(
        self,
    ) -> None:
        """#1078 BLOCKER F3.

        Models an operator manually starting cratedigger.service in the
        window right after the gate hold is taken -- nothing inhibits that
        start pre-hold, and the hold itself only stops units that were
        *already* active at the moment it is taken (this fake's own
        ``metadata_gate("hold manual")``, mirroring the real gate). Without
        re-verifying ``SERVICE_UNITS`` (not the narrower
        ``GATE_STOPPED_UNITS``) after the hold, acquire would reach HELD with
        main still running, right before the migration this hold gates.
        """
        backend = FakeDeployHoldBackend()
        real_ensure_owned_manual_hold = deploy_hold_module._ensure_owned_manual_hold

        def _ensure_then_start_main(b: FakeDeployHoldBackend) -> None:
            real_ensure_owned_manual_hold(b)
            state = b.unit_state(MAIN_SERVICE)
            b.unit_states[MAIN_SERVICE] = UnitState(
                load_state=state.load_state,
                active_state="active",
                sub_state="running",
            )

        with mock.patch.object(
            deploy_hold_module,
            "_ensure_owned_manual_hold",
            side_effect=_ensure_then_start_main,
        ), self.assertRaisesRegex(
            DeployHoldError,
            "timed out waiting for exact services to become stably inactive",
        ):
            acquire_hold(backend)

        # The gate hold WAS taken (maximally quiesced) -- what's missing is
        # re-proof, not the hold itself.
        self.assertTrue(backend.manual_hold)
        self.assertEqual(
            (backend.unit_state(MAIN_SERVICE).active_state, backend.unit_state(MAIN_SERVICE).sub_state),
            ("active", "running"),
        )
        self.assertEqual(backend.phase, "acquiring")


class TestKnownBadPre1078AcquireOrder(unittest.TestCase):
    """#1078: the reorder is load-bearing, not cosmetic."""

    def test_pre_1078_order_deadlocks_on_a_queued_job(self) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
            queue_drain_after_calls=1,
        )

        with self.assertRaisesRegex(DeployHoldError, "active_automation_jobs"):
            _acquire_hold_pre_1078_order(backend)

        # The identical scenario succeeds under the real, reordered acquire.
        reordered = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
            queue_drain_after_calls=1,
        )
        acquire_hold(reordered)
        reordered.assert_default_held()

    def test_pre_must_fix_1_hangs_draining_the_always_on_youtube_daemon(
        self,
    ) -> None:
        """MUST FIX 1 (CRITICAL): the fake defaults YouTube ingest to
        active/running (#1078 MUST FIX 7 -- the real world every acquire
        meets), and nothing before the gate hold ever asks it to stop.
        """
        backend = FakeDeployHoldBackend()
        backend.create_receipt()

        # No timeout patch needed: the fake's clock advances by the real
        # requested duration per sleep() call and its sleep is instant, so
        # the unpatched production _PRODUCER_DRAIN_TIMEOUT_SECONDS/1s-poll
        # bound (21600s -- PRODUCER_SERVICE_UNITS includes cratedigger-
        # unfindable, issue #1112 review round 2) runs here in 21600 fast
        # Python loop iterations at the exact bound production uses.
        with self.assertRaisesRegex(
            DeployHoldError,
            "timed out waiting for exact services to become stably "
            "inactive and job-free",
        ):
            _drain_producers_then_hold_pre_must_fix_1(backend)

        # The deadlock #1078 exists to remove, reproduced by draining
        # YouTube pre-hold: the gate hold was never taken.
        self.assertFalse(backend.manual_hold)

        # The identical scenario succeeds under the real, fixed grouping.
        fixed = FakeDeployHoldBackend()
        acquire_hold(fixed)
        fixed.assert_default_held()


class TestHeldVerification(unittest.TestCase):
    def test_verify_held_is_repeatable_after_switch(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)

        verify_held(backend)
        verify_held(backend)

        backend.assert_default_held()
        self.assertEqual(backend.events.count(("phase", PHASE_HELD)), 3)

    def test_tampered_owned_link_fails_closed(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        backend.control_links[MAIN_TIMER] = "/tmp/not-null"

        with self.assertRaisesRegex(DeployHoldError, "owned control link changed"):
            verify_held(backend)

        self.assertIn(MAIN_TIMER, backend.owned_links)
        self.assertEqual(backend.control_links[MAIN_TIMER], "/tmp/not-null")


class TestStagedRelease(unittest.TestCase):
    def setUp(self) -> None:
        self.backend = FakeDeployHoldBackend()
        acquire_hold(self.backend)

    def test_release_opens_only_the_intended_boundary_at_each_phase(self) -> None:
        prepare_controlled(self.backend)
        self.assertEqual(self.backend.phase, PHASE_PREPARED_CONTROLLED)
        self.assertFalse(self.backend.manual_hold)
        self.assertFalse(self.backend.owned_manual_hold)
        self.assertEqual(
            self.backend.started_units,
            [*CONTROLLED_WORKER_UNITS, MAIN_SERVICE],
        )
        self.assertEqual(
            self.backend.owned_inhibitors,
            {YOUTUBE_SERVICE},
        )
        self.assertEqual(
            self.backend.inhibitor_files,
            {YOUTUBE_SERVICE},
        )
        self.assertIn(("metadata-gate", "resume-if-clear"), self.backend.events)
        release_index = self.backend.events.index(
            ("metadata-gate", "release manual")
        )
        for service in START_INHIBITORS:
            self.assertLess(
                self.backend.events.index(("inhibitor-create", service)),
                release_index,
            )
        self.assertEqual(self.backend.owned_links, set(TIMER_UNITS))

        # PR1 verifies the controlled invocation before this transition.
        open_main_timer(self.backend)
        self.assertEqual(self.backend.phase, PHASE_MAIN_TIMER_OPEN)
        self.assertNotIn(MAIN_TIMER, self.backend.owned_links)
        self.assertNotIn(MAIN_TIMER, self.backend.control_links)
        self.assertEqual(self.backend.unit_state(MAIN_TIMER).load_state, "loaded")
        self.assertEqual(
            self.backend.started_units,
            [*CONTROLLED_WORKER_UNITS, MAIN_SERVICE, MAIN_TIMER],
        )
        for timer in TIMER_UNITS:
            if timer != MAIN_TIMER:
                self.assertEqual(self.backend.unit_state(timer).load_state, "masked")

        # PR1 capture-target returns this ID before the ordinary cycle finishes.
        finish_release(self.backend, INVOCATION)
        self.assertEqual(self.backend.phase, PHASE_COMPLETE_PENDING)
        self.assertEqual(self.backend.ordinary_invocation, INVOCATION)
        self.assertEqual(self.backend.owned_links, set())
        self.assertEqual(self.backend.control_links, {})
        self.assertEqual(self.backend.inhibitor_files, set())
        self.assertEqual(self.backend.owned_inhibitors, set())
        self.assertIn(("metadata-gate", "resume-if-clear"), self.backend.events)
        self.assertEqual(
            self.backend.started_units,
            [*CONTROLLED_WORKER_UNITS, MAIN_SERVICE, *TIMER_UNITS],
        )

        # PR1 verify-exact proves this same invocation before completion.
        complete_release(self.backend, INVOCATION)
        self.assertFalse(self.backend.receipt)
        self.assertEqual(self.backend.control_links, {})

    def test_open_main_timer_refuses_a_tampered_owned_link(self) -> None:
        prepare_controlled(self.backend)
        self.backend.control_links[MAIN_TIMER] = "/tmp/tampered"

        with self.assertRaisesRegex(DeployHoldError, "owned control link changed"):
            open_main_timer(self.backend)

        self.assertIn(MAIN_TIMER, self.backend.owned_links)

    def test_complete_requires_the_captured_ordinary_invocation(self) -> None:
        prepare_controlled(self.backend)
        open_main_timer(self.backend)
        finish_release(self.backend, INVOCATION)

        with self.assertRaisesRegex(DeployHoldError, "invocation does not match"):
            complete_release(self.backend, "b" * 32)

        self.assertTrue(self.backend.receipt)

    def test_complete_resumes_after_atomic_receipt_retirement(self) -> None:
        backend = FakeDeployHoldBackend(interrupt_receipt_retirement=True)
        acquire_hold(backend)
        prepare_controlled(backend)
        open_main_timer(backend)
        finish_release(backend, INVOCATION)

        with self.assertRaisesRegex(InterruptedError, "retirement interruption"):
            complete_release(backend, INVOCATION)

        self.assertFalse(backend.receipt)
        self.assertTrue(backend.retired_receipt)
        complete_release(backend, INVOCATION)
        self.assertFalse(backend.retired_receipt)

    def test_phase_order_fails_closed(self) -> None:
        with self.assertRaisesRegex(DeployHoldError, "expected phase"):
            open_main_timer(self.backend)

    def test_prepare_rejects_an_unowned_producer_inhibitor(self) -> None:
        self.backend.inhibitor_files.add(MAIN_SERVICE)

        with self.assertRaisesRegex(
            DeployHoldError,
            "unowned producer inhibitor",
        ):
            prepare_controlled(self.backend)

        self.assertTrue(self.backend.manual_hold)

    def test_prepare_fails_fast_if_a_dependency_hold_appears(self) -> None:
        self.backend.other_metadata_holds.add("dependency")

        with self.assertRaisesRegex(
            DeployHoldError,
            "metadata gate became held",
        ):
            prepare_controlled(self.backend)

        self.assertEqual(
            self.backend.owned_inhibitors,
            set(START_INHIBITORS),
        )
        self.assertEqual(
            self.backend.inhibitor_files,
            set(START_INHIBITORS),
        )

    def test_recovery_removes_only_receipt_owned_inhibitors(self) -> None:
        prepare_controlled(self.backend)

        recover_held(self.backend)

        self.assertEqual(self.backend.inhibitor_files, set())
        self.assertEqual(self.backend.owned_inhibitors, set())
        self.backend.assert_default_held()

    def test_recovery_clears_an_unmaterialized_owned_inhibitor_intent(
        self,
    ) -> None:
        self.backend.mark_inhibitor_owned(MAIN_SERVICE)

        recover_held(self.backend)

        self.assertEqual(self.backend.inhibitor_files, set())
        self.assertEqual(self.backend.owned_inhibitors, set())
        self.backend.assert_default_held()

    def test_recover_held_reestablishes_every_boundary_from_release(self) -> None:
        prepare_controlled(self.backend)
        open_main_timer(self.backend)
        finish_release(self.backend, INVOCATION)

        recover_held(self.backend)

        self.backend.assert_default_held()
        self.assertIsNone(self.backend.ordinary_invocation)
        self.assertEqual(
            self.backend.control_links,
            {timer: "/dev/null" for timer in TIMER_UNITS},
        )

    def test_recovery_from_complete_pending_preserves_the_captured_successor_on_failure(
        self,
    ) -> None:
        """A recover-held that fails mid-branch must not destroy state
        before the boundary it belongs to is re-proven -- forward hygiene,
        not a live escape hatch: complete_release is refused anyway here
        (the manual hold is already re-owned by the time _clear_owned_inhibitors
        fails), so nothing downstream reads the preserved marker today.
        """
        prepare_controlled(self.backend)
        open_main_timer(self.backend)
        finish_release(self.backend, INVOCATION)
        self.assertEqual(self.backend.ordinary_invocation, INVOCATION)
        # _clear_owned_inhibitors is the LAST step of the else branch, so an
        # unowned inhibitor here fails recovery only after
        # _ensure_owned_manual_hold and the SERVICE_UNITS drain already ran.
        self.backend.inhibitor_files.add(MAIN_SERVICE)

        with self.assertRaisesRegex(DeployHoldError, "unowned producer inhibitor"):
            recover_held(self.backend)

        self.assertEqual(self.backend.phase, PHASE_COMPLETE_PENDING)
        self.assertEqual(self.backend.ordinary_invocation, INVOCATION)
        self.assertEqual(self.backend.read_ordinary_invocation(), INVOCATION)

    def test_invocation_id_must_be_exact_systemd_shape(self) -> None:
        prepare_controlled(self.backend)
        open_main_timer(self.backend)

        for invalid in ("", "none", "xyz", "a" * 31, "a" * 33):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(DeployHoldError, "InvocationID"):
                finish_release(self.backend, invalid)


class TestRecoveryReprovesAnUnprovenAcquisition(unittest.TestCase):
    """A receipt reaches HELD only through the acquire preconditions."""

    def test_recovery_from_acquiring_refuses_an_anomaly_lifecycle_field(
        self,
    ) -> None:
        """malformed_enqueued_at_rows is an anomaly -- nothing drains it, so
        both acquire and recovery must fail fast rather than wait, and still
        leave the full boundary re-established (masks + gate hold) since the
        failure is the final _assert_clean_old_lifecycle check.
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(0, 0, 0, 1),
        )
        with self.assertRaisesRegex(
            DeployHoldError,
            "malformed_enqueued_at_rows",
        ):
            acquire_hold(backend)
        self.assertEqual(backend.phase, PHASE_ACQUIRING)

        with self.assertRaisesRegex(
            DeployHoldError,
            "old lifecycle is not clean",
        ):
            recover_held(backend)

        self.assertEqual(backend.phase, PHASE_ACQUIRING)
        # Recovery still re-established the strictest boundary before refusing
        # to promote the unproven receipt.
        self.assertTrue(backend.manual_hold)
        self.assertTrue(backend.owned_manual_hold)
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        self.assertEqual(
            backend.control_links,
            {timer: "/dev/null" for timer in TIMER_UNITS},
        )
        self.assertEqual(backend.inhibitor_files, set())

    def test_recovery_from_acquiring_drains_a_queued_job_before_holding(
        self,
    ) -> None:
        """recover_held's acquiring branch shares acquire_hold's producer-
        drain-before-hold order -- a queued job still lets it reach HELD."""
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
            queue_drain_after_calls=1,
        )
        backend.create_receipt()

        recover_held(backend)

        backend.assert_default_held()
        self.assertEqual(backend.owned_inhibitor_units(), ())

    def test_recovery_from_acquiring_refuses_a_stale_start_contract(self) -> None:
        backend = FakeDeployHoldBackend(
            controlled_start_contract_current=False,
        )
        backend.create_receipt()

        with self.assertRaisesRegex(
            DeployHoldError,
            "controlled-start prerequisite changed",
        ):
            recover_held(backend)

        self.assertEqual(backend.phase, PHASE_ACQUIRING)
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))

    def test_recovery_from_a_clean_acquiring_receipt_still_reaches_held(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.mark_link_owned(MAIN_TIMER)

        recover_held(backend)

        backend.assert_default_held()
        self.assertIn(("lifecycle-preflight",), backend.events)
        self.assertEqual(
            backend.control_links,
            {timer: "/dev/null" for timer in TIMER_UNITS},
        )

    def test_recovery_after_the_migration_never_requeries_the_old_lifecycle(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        # Post-migration reality: the controlled cycle legitimately enqueues
        # automation work and the old-lifecycle schema this preflight reads has
        # already been migrated. Recovery exists to restore safety, so it must
        # not demand the pre-migration quiescence proof a second time.
        backend.preflight = LifecyclePreflight(3, 1, 2, 1)
        backend.controlled_start_contract_current = False
        preflight_calls = backend.events.count(("lifecycle-preflight",))

        recover_held(backend)

        backend.assert_default_held()
        self.assertEqual(
            backend.events.count(("lifecycle-preflight",)),
            preflight_calls,
        )


class TestAbortHold(unittest.TestCase):
    """The escape hatch recover_held cannot offer: return to unheld operation."""

    def test_abort_from_acquiring_restores_ordinary_operation(self) -> None:
        # recovery_required_jobs is an anomaly -- acquire fails forever, and
        # recover_held would re-prove the identical, unfixable precondition.
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(0, 1, 0, 0),
        )
        with self.assertRaises(DeployHoldError):
            acquire_hold(backend)
        self.assertEqual(backend.phase, PHASE_ACQUIRING)

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertFalse(backend.retired_receipt)
        self.assertFalse(backend.manual_hold)
        self.assertFalse(backend.owned_manual_hold)
        self.assertEqual(backend.owned_links, set())
        self.assertEqual(backend.control_links, {})
        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        for timer in TIMER_UNITS:
            state = backend.unit_state(timer)
            self.assertEqual((state.load_state, state.active_state), ("loaded", "active"))
        # #1078 MUST FIX 2: YouTube ingest is gate-stopped too (MUST FIX 1),
        # so abort must restart it, not just the three controlled workers.
        for service in (*CONTROLLED_WORKER_UNITS, YOUTUBE_SERVICE):
            self.assertEqual(backend.unit_state(service).active_state, "active")

    def test_abort_restarts_youtube_ingest_and_verifies_it_actually_came_up(
        self,
    ) -> None:
        """#1078 MUST FIX 2: youtube-ingest is Type=simple, wantedBy=multi-
        user.target, Restart=on-failure, no timer -- a clean stop by the
        gate hold is not a failure, so nothing restarts it except an
        explicit start. Releasing the hold restarts nothing by itself.
        """
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "inactive")

        abort_hold(backend)

        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")
        self.assertIn(YOUTUBE_SERVICE, backend.started_units)

    def test_abort_fails_loudly_on_a_foreign_gate_hold(self) -> None:
        """#1078 BLOCKER F1: refused before our own hold is ever released.

        A foreign hold (e.g. the monthly discogs-import hold) blocks every
        gate-guarded ExecCondition, so a bare systemctl start is a silent
        no-op. Without the up-front check, abort would release our manual
        hold, discover the foreign hold only inside the restart-verification
        wait, and exit non-zero with our hold already gone -- the receipt
        stuck claiming "held" while nothing blocks the foreign hold's own
        eventual resume from starting every guarded unit, including a main
        cycle, underneath it.
        """
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        # A hold unrelated to this receipt, already present when abort runs.
        backend.other_metadata_holds.add("discogs-import")

        with self.assertRaisesRegex(
            DeployHoldError, "foreign metadata gate holds block abort",
        ):
            abort_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertTrue(backend.manual_hold)
        self.assertTrue(backend.owned_manual_hold)
        self.assertEqual(
            [event for event in backend.events if event[0] == "metadata-gate"],
            [("metadata-gate", "hold manual")],
        )
        for service in CONTROLLED_WORKER_UNITS:
            self.assertEqual(backend.unit_state(service).active_state, "inactive")

    def test_abort_retries_after_an_interrupted_timer_restart(self) -> None:
        """#1078 MUST FIX 3: restart before disowning.

        Injects a real interruption inside abort_hold's own timer-restart
        block (right after masks are removed, before start/verify), so this
        exercises abort_hold's own ordering rather than a hand-built "already
        interrupted" state. Restart-before-disown means a retry still owns
        the timers and finishes the job; disown-before-restart (the exact
        defect this fixes) would instead silently remove the receipt next
        with search cadence dead, since a retry sees "nothing owned."
        """
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        backend.assert_default_held()

        real_assert_load_states = deploy_hold_module._assert_load_states
        with (
            mock.patch.object(
                deploy_hold_module,
                "_assert_load_states",
                side_effect=InterruptedError("injected mid-restart interruption"),
            ),
            self.assertRaisesRegex(InterruptedError, "mid-restart interruption"),
        ):
            abort_hold(backend)

        # The masks are gone (removed before the injected interruption) but
        # ownership is still retained, and the timers are still stopped.
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        self.assertEqual(backend.control_links, {})
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "inactive")
        self.assertTrue(backend.receipt)

        with mock.patch.object(
            deploy_hold_module,
            "_assert_load_states",
            side_effect=real_assert_load_states,
        ):
            abort_hold(backend)

        self.assertFalse(backend.receipt)
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")

    def test_abort_while_the_automation_queue_never_drains(self) -> None:
        """A drainable-field timeout is recoverable too, not just an anomaly."""
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
        )
        # No timeout patch needed -- see test_acquire_times_out_waiting_for_a_
        # queue_that_never_drains.
        with self.assertRaises(DeployHoldError):
            acquire_hold(backend)
        self.assertEqual(backend.phase, PHASE_ACQUIRING)
        # Interrupted mid-wait: the gate hold was never taken, and (#1078
        # MUST FIX 5) the pre-hold window owns no inhibitor at all.
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.owned_inhibitors, set())

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertEqual(backend.owned_links, set())
        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")

    def test_abort_from_held_restores_ordinary_operation(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        backend.assert_default_held()

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.owned_links, set())
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")
        for service in (*CONTROLLED_WORKER_UNITS, YOUTUBE_SERVICE):
            self.assertEqual(backend.unit_state(service).active_state, "active")

    def test_abort_from_prepared_controlled_restores_ordinary_operation(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.owned_links, set())
        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")
        # #1078 BLOCKER F2: prepare_controlled already unmarked the manual
        # hold at this phase, so only the owned YouTube inhibitor was ever
        # blocking it -- removing the inhibitor alone does not start a
        # stopped Type=simple unit.
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")

    def test_abort_from_main_timer_open_restores_ordinary_operation(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        open_main_timer(backend)

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertEqual(backend.owned_links, set())
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")
        # #1078 BLOCKER F2
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")

    def test_abort_from_complete_pending_restores_ordinary_operation(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        open_main_timer(backend)
        finish_release(backend, INVOCATION)

        abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertEqual(backend.owned_links, set())
        self.assertEqual(backend.owned_inhibitors, set())
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")

    def test_abort_resumes_after_atomic_receipt_retirement(self) -> None:
        backend = FakeDeployHoldBackend(interrupt_receipt_retirement=True)
        acquire_hold(backend)

        with self.assertRaisesRegex(InterruptedError, "retirement interruption"):
            abort_hold(backend)

        self.assertFalse(backend.receipt)
        self.assertTrue(backend.retired_receipt)
        abort_hold(backend)
        self.assertFalse(backend.retired_receipt)

    def test_abort_on_a_missing_receipt_fails_closed(self) -> None:
        backend = FakeDeployHoldBackend()

        with self.assertRaisesRegex(DeployHoldError, "receipt is missing"):
            abort_hold(backend)

    def test_abort_refuses_an_unknown_phase(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.write_phase("some-unrecognized-phase")

        with self.assertRaisesRegex(
            DeployHoldError, "cannot abort unknown phase",
        ):
            abort_hold(backend)

        self.assertTrue(backend.receipt)

    def test_abort_fails_closed_on_an_unowned_inhibitor(self) -> None:
        """#1078 MUST FIX 4: validated before any mutation -- nothing moves."""
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        # Simulates external/operator state this receipt never created.
        backend.inhibitor_files.add(MAIN_SERVICE)

        with self.assertRaisesRegex(
            DeployHoldError, "unowned producer inhibitor",
        ):
            abort_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertTrue(backend.manual_hold)
        self.assertTrue(backend.owned_manual_hold)
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        for service in CONTROLLED_WORKER_UNITS:
            self.assertEqual(backend.unit_state(service).active_state, "inactive")

    def test_abort_fails_closed_on_an_unowned_control_link(self) -> None:
        """#1078 MUST FIX 4: validated before any mutation -- nothing moves."""
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.mark_link_owned(MAIN_TIMER)
        backend.create_control_mask(MAIN_TIMER)
        # Simulates external/operator state this receipt never owned.
        backend.control_links[WATCHDOG_TIMER] = "/dev/null"

        with self.assertRaisesRegex(DeployHoldError, "unowned control path"):
            abort_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.control_links, {
            MAIN_TIMER: "/dev/null", WATCHDOG_TIMER: "/dev/null",
        })
        self.assertIn(MAIN_TIMER, backend.owned_links)

    def test_abort_never_releases_an_unowned_manual_hold(self) -> None:
        # Interrupted before any ownership marker was ever written -- while
        # a pre-existing, unrelated hold happens to be active.
        backend = FakeDeployHoldBackend(manual_hold=True)
        backend.create_receipt()

        abort_hold(backend)

        self.assertTrue(backend.manual_hold)
        self.assertFalse(backend.receipt)

    def test_abort_owning_the_main_inhibitor_does_not_hang_on_the_oneshot(
        self,
    ) -> None:
        """#1096 correction round, M1 mirror: a receipt can own MAIN_SERVICE's
        own inhibitor too -- interrupted mid prepare_controlled, after both
        inhibitors are created but before prepare_controlled's own
        ``_release_owned_inhibitor(MAIN_SERVICE)`` runs near the end of that
        function. MAIN_SERVICE is Type=oneshot -- it runs one cycle and
        exits, so waiting for it to become active/running (as the pre-fix
        code did for every owned inhibitor without exception) hangs for the
        full drain timeout, every time.
        """
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.write_phase(PHASE_HELD)
        backend.mark_inhibitor_owned(MAIN_SERVICE)
        backend.create_start_inhibitor(MAIN_SERVICE)
        backend.mark_inhibitor_owned(YOUTUBE_SERVICE)
        backend.create_start_inhibitor(YOUTUBE_SERVICE)
        backend.unit_states[YOUTUBE_SERVICE] = UnitState(
            load_state="loaded", active_state="inactive", sub_state="dead",
        )

        abort_hold(backend)

        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")
        self.assertIn(MAIN_SERVICE, backend.started_units)
        self.assertFalse(backend.receipt)

    def test_abort_owning_manual_hold_and_youtube_inhibitor_together_does_not_hang(
        self,
    ) -> None:
        """#1096 correction round, M2 mirror: the manual hold and an
        inhibitor can be owned together -- interrupted mid
        prepare_controlled, after both inhibitors are created but before
        the manual hold is released. Releasing the hold and starting
        GATE_STOPPED_UNITS (which includes YouTube) before YouTube's own
        inhibitor is removed silently skips its start (still condition-
        blocked) and hangs the restart-verification wait forever.
        """
        backend = FakeDeployHoldBackend(manual_hold=True)
        backend.create_receipt()
        backend.write_phase(PHASE_HELD)
        backend.mark_manual_hold_owned()
        backend.mark_inhibitor_owned(YOUTUBE_SERVICE)
        backend.create_start_inhibitor(YOUTUBE_SERVICE)
        for service in GATE_STOPPED_UNITS:
            backend.unit_states[service] = UnitState(
                load_state="loaded", active_state="inactive", sub_state="dead",
            )

        abort_hold(backend)

        self.assertFalse(backend.owned_manual_hold)
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        for service in GATE_STOPPED_UNITS:
            self.assertEqual(backend.unit_state(service).active_state, "active")
        self.assertFalse(backend.receipt)

    def test_abort_refuses_a_foreign_hold_coincidentally_named_manual(
        self,
    ) -> None:
        """#1096 correction round, S1 mirror: a hold reason coincidentally
        named "manual" that this receipt does NOT own is exactly as
        foreign as any other reason -- every gate-guarded unit's start
        condition requires the ENTIRE holds directory empty, not just that
        no reason other than "manual" is present. Refusing before any
        mutation is what keeps abort's own "a refusal never leaves the
        boundary half torn down" contract true.
        """
        backend = FakeDeployHoldBackend(manual_hold=True)
        backend.create_receipt()
        backend.write_phase(PHASE_HELD)
        backend.mark_inhibitor_owned(YOUTUBE_SERVICE)
        backend.create_start_inhibitor(YOUTUBE_SERVICE)

        with self.assertRaisesRegex(
            DeployHoldError, "foreign metadata gate holds block abort",
        ):
            abort_hold(backend)

        self.assertEqual(backend.inhibitor_files, {YOUTUBE_SERVICE})
        self.assertEqual(backend.owned_inhibitors, {YOUTUBE_SERVICE})
        self.assertTrue(backend.manual_hold)
        self.assertTrue(backend.receipt)
        self.assertEqual(backend.started_units, [])


class TestReceiptlessAbortAdoptsPersistentMarkers(unittest.TestCase):
    """#1096: persistent ownership markers survive a reboot; a receiptless
    abort adopts exactly the objects they mark, restoring ordinary unheld
    operation with no dead end. Options 1+4 per issue comment
    5266609958 -- option 2 (persistent receipt) and option 3 (ownership
    encoded in the object's own content) are rejected.
    """

    def test_reboot_at_prepared_controlled_abort_adopts_and_restores_ordinary_operation(
        self,
    ) -> None:
        """Pin (a): the phase the module-vm.nix first deploy-hold scenario
        reaches at prepare-controlled. prepare_controlled already released
        the manual hold (and its persistent marker) before writing this
        phase, so only the YouTube inhibitor -- and its new persistent
        sibling marker -- survive the reboot.
        """
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        self.assertEqual(backend.persistent_inhibitor_markers, {YOUTUBE_SERVICE})
        self.assertFalse(backend.persistent_manual_marker)

        backend.reboot()

        self.assertFalse(backend.receipt_exists())
        self.assertTrue(backend.persistent_inhibitor_marker_exists(YOUTUBE_SERVICE))
        self.assertIn(YOUTUBE_SERVICE, backend.inhibitor_files)
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "inactive")

        abort_hold(backend)

        self.assertFalse(backend.receipt_exists())
        self.assertFalse(backend.retired_receipt_exists())
        self.assertEqual(backend.owned_inhibitors, set())
        self.assertEqual(backend.inhibitor_files, set())
        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertFalse(backend.persistent_manual_marker)
        self.assertFalse(backend.manual_hold)
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")
        for service in (*CONTROLLED_WORKER_UNITS, YOUTUBE_SERVICE):
            self.assertEqual(backend.unit_state(service).active_state, "active")

    def test_reboot_at_held_abort_adopts_the_manual_hold_and_restores_ordinary_operation(
        self,
    ) -> None:
        """Any post-HELD phase is exposed, not only prepare-controlled
        onward: a reboot right after acquire reaches HELD leaves the manual
        hold -- not yet released -- as the surviving persistent object.
        """
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        self.assertTrue(backend.persistent_manual_marker)
        self.assertEqual(backend.persistent_inhibitor_markers, set())

        backend.reboot()

        self.assertFalse(backend.receipt_exists())
        self.assertTrue(backend.persistent_manual_marker_exists())
        self.assertTrue(backend.manual_hold)

        abort_hold(backend)

        self.assertFalse(backend.persistent_manual_marker)
        self.assertFalse(backend.manual_hold)
        self.assertFalse(backend.receipt_exists())
        for service in (*CONTROLLED_WORKER_UNITS, YOUTUBE_SERVICE):
            self.assertEqual(backend.unit_state(service).active_state, "active")
        for timer in TIMER_UNITS:
            self.assertEqual(backend.unit_state(timer).active_state, "active")

    def test_clean_boot_abort_refuses_nothing_owned(self) -> None:
        """Pin (b): a clean boot with no prior deploy hold must never turn
        into a mass restart."""
        backend = FakeDeployHoldBackend()

        with self.assertRaisesRegex(
            DeployHoldError,
            "no persistent ownership markers exist to adopt",
        ):
            abort_hold(backend)

        self.assertEqual(backend.started_units, [])

    def test_receiptless_abort_refuses_a_foreign_unmarked_inhibitor(self) -> None:
        """Pin (c): an unmarked object present alongside a marked one is
        refused loudly, and nothing -- marked or foreign -- is touched."""
        backend = FakeDeployHoldBackend(
            inhibitor_files={YOUTUBE_SERVICE, MAIN_SERVICE},
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )

        with self.assertRaises(DeployHoldError) as caught:
            abort_hold(backend)
        self.assertEqual(
            str(caught.exception),
            f"unowned producer inhibitor exists for {MAIN_SERVICE}",
        )

        self.assertEqual(backend.inhibitor_files, {YOUTUBE_SERVICE, MAIN_SERVICE})
        self.assertEqual(backend.persistent_inhibitor_markers, {YOUTUBE_SERVICE})
        self.assertEqual(backend.started_units, [])

    def test_receiptless_abort_cleans_an_orphan_marker_whose_object_is_absent(
        self,
    ) -> None:
        """Pin (d): crashed between the marker write and the inhibitor's
        own creation -- the object never actually came to exist."""
        backend = FakeDeployHoldBackend(
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )
        backend.unit_states[YOUTUBE_SERVICE] = UnitState(
            load_state="loaded", active_state="inactive", sub_state="dead",
        )
        self.assertNotIn(YOUTUBE_SERVICE, backend.inhibitor_files)

        abort_hold(backend)

        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")
        self.assertIn(YOUTUBE_SERVICE, backend.started_units)

    def test_retired_receipt_plus_an_unrelated_orphan_marker_takes_two_runs(
        self,
    ) -> None:
        """#1096 correction round, N6: a wholly unrelated, earlier orphan
        persistent marker can coexist with an interrupted retirement from a
        SEPARATE, later receipt. The first abort finishes only the
        retirement it owns; the marker is untouched. The second abort --
        now hitting neither a receipt nor a retired one -- reaches
        receiptless adoption on its own. Two runs, not a dead end.
        """
        backend = FakeDeployHoldBackend(
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )
        backend.unit_states[YOUTUBE_SERVICE] = UnitState(
            load_state="loaded", active_state="inactive", sub_state="dead",
        )
        backend.retired_receipt = True

        abort_hold(backend)

        self.assertFalse(backend.retired_receipt)
        self.assertEqual(backend.persistent_inhibitor_markers, {YOUTUBE_SERVICE})
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "inactive")

        abort_hold(backend)

        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")

    def test_reboot_plus_our_marker_plus_a_foreign_hold_fails_loudly_then_rerun_finishes(
        self,
    ) -> None:
        """Pin (e): a foreign hold blocks adoption exactly like it blocks a
        receipt-owned abort; every marker is retained so a rerun after the
        operator clears the foreign hold finishes the job."""
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        backend.reboot()
        backend.other_metadata_holds.add("discogs-import")

        with self.assertRaisesRegex(
            DeployHoldError, "foreign metadata gate holds block abort",
        ):
            abort_hold(backend)

        self.assertEqual(backend.persistent_inhibitor_markers, {YOUTUBE_SERVICE})
        self.assertEqual(backend.inhibitor_files, {YOUTUBE_SERVICE})
        self.assertEqual(backend.started_units, [])

        backend.other_metadata_holds.discard("discogs-import")
        abort_hold(backend)

        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertEqual(backend.inhibitor_files, set())
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")

    def test_acquire_refuses_a_marked_manual_hold_and_points_to_abort(self) -> None:
        backend = FakeDeployHoldBackend(
            manual_hold=True, persistent_manual_marker=True,
        )

        with self.assertRaisesRegex(DeployHoldError, "run 'abort'"):
            acquire_hold(backend)

    def test_acquire_refuses_an_unmarked_manual_hold_with_the_original_message(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(manual_hold=True)

        with self.assertRaises(DeployHoldError) as caught:
            acquire_hold(backend)
        self.assertEqual(
            str(caught.exception), "unowned manual hold already exists",
        )

    def test_acquire_refuses_a_marked_inhibitor_and_points_to_abort(self) -> None:
        backend = FakeDeployHoldBackend(
            inhibitor_files={YOUTUBE_SERVICE},
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )

        with self.assertRaisesRegex(DeployHoldError, "run 'abort'"):
            acquire_hold(backend)

    def test_acquire_refuses_an_unmarked_inhibitor_with_the_original_message(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(inhibitor_files={YOUTUBE_SERVICE})

        with self.assertRaises(DeployHoldError) as caught:
            acquire_hold(backend)
        self.assertEqual(
            str(caught.exception),
            f"unowned producer inhibitor already exists for {YOUTUBE_SERVICE}",
        )

    def test_mark_manual_hold_owned_persists_the_marker_before_the_tmpfs_marker(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend()

        backend.mark_manual_hold_owned()

        self.assertEqual(
            [event[0] for event in backend.events],
            ["persist-own-manual", "own-manual"],
        )

    def test_ensure_owned_manual_hold_persists_the_marker_before_taking_the_gate(
        self,
    ) -> None:
        """Fault-injection qualified: swapping mark_manual_hold_owned() and
        the metadata_gate('hold manual') call in _ensure_owned_manual_hold
        makes this fail (verified by hand; #1096 report)."""
        backend = FakeDeployHoldBackend()

        _ensure_owned_manual_hold(backend)

        self.assertEqual(
            list(backend.events),
            [
                ("persist-own-manual",),
                ("own-manual",),
                ("metadata-gate", "hold manual"),
            ],
        )

    def test_ensure_owned_start_inhibitor_persists_the_marker_before_creating_the_object(
        self,
    ) -> None:
        """Fault-injection qualified: swapping mark_inhibitor_owned() and
        create_start_inhibitor() in _ensure_owned_start_inhibitor makes this
        fail (verified by hand; #1096 report)."""
        backend = FakeDeployHoldBackend()

        _ensure_owned_start_inhibitor(backend, YOUTUBE_SERVICE)

        self.assertEqual(
            list(backend.events),
            [
                ("persist-own-inhibitor", YOUTUBE_SERVICE),
                ("own-inhibitor", YOUTUBE_SERVICE),
                ("inhibitor-create", YOUTUBE_SERVICE),
            ],
        )

    def test_adoption_removes_the_persistent_inhibitor_marker_only_after_restart_is_proven(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(
            inhibitor_files={YOUTUBE_SERVICE},
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )
        backend.unit_states[YOUTUBE_SERVICE] = UnitState(
            load_state="loaded", active_state="inactive", sub_state="dead",
        )

        abort_hold(backend)

        kinds = list(backend.events)
        remove_index = kinds.index(("inhibitor-remove", YOUTUBE_SERVICE))
        start_index = kinds.index(("start", YOUTUBE_SERVICE))
        disown_index = kinds.index(("persist-disown-inhibitor", YOUTUBE_SERVICE))
        self.assertLess(remove_index, start_index)
        self.assertLess(start_index, disown_index)

    def test_adoption_removes_the_persistent_manual_marker_only_after_release_and_restart_are_proven(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(
            manual_hold=True, persistent_manual_marker=True,
        )
        for service in GATE_STOPPED_UNITS:
            backend.unit_states[service] = UnitState(
                load_state="loaded", active_state="inactive", sub_state="dead",
            )

        abort_hold(backend)

        kinds = list(backend.events)
        release_index = kinds.index(("metadata-gate", "release manual"))
        disown_index = kinds.index(("persist-disown-manual",))
        self.assertLess(release_index, disown_index)
        start_indices = [
            index
            for index, event in enumerate(kinds)
            if event[0] == "start" and event[1] in GATE_STOPPED_UNITS
        ]
        self.assertTrue(start_indices)
        self.assertLess(max(start_indices), disown_index)

    def test_main_and_youtube_marked_together_abort_adopts_without_hanging(
        self,
    ) -> None:
        """#1096 correction round, M1: MAIN_SERVICE can be marked too --
        reachable from a reboot mid prepare_controlled, after both
        inhibitors are created but before prepare_controlled releases
        MAIN's own again near the end of that function. MAIN_SERVICE is
        Type=oneshot -- it runs one cycle and exits, never reaching
        active/running -- so a restart-verification wait that includes it
        (the pre-fix shape) hangs for the full drain timeout every time,
        having already deleted both inhibitor files.
        """
        backend = FakeDeployHoldBackend(
            inhibitor_files={MAIN_SERVICE, YOUTUBE_SERVICE},
            persistent_inhibitor_markers={MAIN_SERVICE, YOUTUBE_SERVICE},
        )
        backend.unit_states[YOUTUBE_SERVICE] = UnitState(
            load_state="loaded", active_state="inactive", sub_state="dead",
        )

        abort_hold(backend)

        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertEqual(backend.inhibitor_files, set())
        self.assertEqual(backend.unit_state(YOUTUBE_SERVICE).active_state, "active")
        self.assertIn(MAIN_SERVICE, backend.started_units)
        self.assertIn(YOUTUBE_SERVICE, backend.started_units)

    def test_manual_and_youtube_marked_together_abort_adopts_without_hanging(
        self,
    ) -> None:
        """#1096 correction round, M2: the manual hold and an inhibitor can
        be marked together -- reachable from a reboot mid
        prepare_controlled, after both inhibitors are created but before
        the manual hold is released. Releasing the hold and starting
        GATE_STOPPED_UNITS (which includes YouTube) before YouTube's own
        inhibitor is removed (the pre-fix shape) silently skips its start
        and hangs the proof forever.
        """
        backend = FakeDeployHoldBackend(
            manual_hold=True,
            persistent_manual_marker=True,
            inhibitor_files={YOUTUBE_SERVICE},
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )
        for service in GATE_STOPPED_UNITS:
            backend.unit_states[service] = UnitState(
                load_state="loaded", active_state="inactive", sub_state="dead",
            )

        abort_hold(backend)

        self.assertFalse(backend.persistent_manual_marker)
        self.assertFalse(backend.manual_hold)
        self.assertEqual(backend.persistent_inhibitor_markers, set())
        self.assertEqual(backend.inhibitor_files, set())
        for service in GATE_STOPPED_UNITS:
            self.assertEqual(backend.unit_state(service).active_state, "active")

    def test_foreign_manual_hold_blocks_receiptless_adoption_before_any_mutation(
        self,
    ) -> None:
        """#1096 correction round, S1: a hold reason coincidentally named
        "manual" that we do NOT hold the persistent marker for is exactly
        as foreign as any other reason -- every gate-guarded unit's start
        condition requires the ENTIRE holds directory empty, not just that
        no reason other than "manual" is present. Refusing here, before any
        mutation, is what keeps this module's "a refusal leaves every
        marker and object exactly as found" contract true.
        """
        backend = FakeDeployHoldBackend(
            manual_hold=True,  # foreign: no persistent_manual_marker
            inhibitor_files={YOUTUBE_SERVICE},
            persistent_inhibitor_markers={YOUTUBE_SERVICE},
        )

        with self.assertRaisesRegex(
            DeployHoldError, "foreign metadata gate holds block abort",
        ):
            abort_hold(backend)

        self.assertEqual(backend.inhibitor_files, {YOUTUBE_SERVICE})
        self.assertEqual(backend.persistent_inhibitor_markers, {YOUTUBE_SERVICE})
        self.assertTrue(backend.manual_hold)
        self.assertEqual(backend.started_units, [])


class TestGateGuardModelDerivation(unittest.TestCase):
    """#1100 item 1: the gate's guarded/resume sets are named constants, and
    GATE_GUARDED_LINE/GATE_RESUME_LINE -- the exact module-level values
    verify_controlled_start_contract compares against the live gate config,
    not a re-composition of them -- are byte-identical to the hardcoded
    strings they replace. Pinning the bare names (not calling _units_line
    again from the test with test-chosen arguments) is what would catch a
    future call-site mis-wire (e.g. building the guarded line from
    GATE_RESUME_UNITS): that one composition now happens exactly once, at
    import time, so there is nothing left for verify_controlled_start_contract
    to get wrong about those two literals independently of what this test
    already pinned. That scope is narrow, not the whole method: the
    condition-path presence counts, the ExecCondition path regex, the
    shared-gate-path uniqueness check, the controlled-worker
    inhibitor-absence check, and the splitlines().count() == 1 comparison
    itself have no unit coverage here -- only `nix build
    .#checks.x86_64-linux.moduleVm` and a live acquire exercise those.
    """

    def test_guarded_units_includes_the_main_timer_and_service(self) -> None:
        self.assertIn(MAIN_TIMER, GATE_GUARDED_UNITS)
        self.assertIn(MAIN_SERVICE, GATE_GUARDED_UNITS)
        self.assertEqual(set(GATE_GUARDED_UNITS), set(GATE_RESUME_UNITS))

    def test_expected_guarded_and_resume_lines_are_byte_identical_to_the_original_literal(
        self,
    ) -> None:
        self.assertEqual(
            GATE_GUARDED_LINE,
            "guarded_units=(cratedigger.timer cratedigger.service "
            "cratedigger-web.service cratedigger-importer.service "
            "cratedigger-import-preview-worker.service "
            "cratedigger-youtube-ingest.service)",
        )
        self.assertEqual(
            GATE_RESUME_LINE,
            "resume_units=(cratedigger.service cratedigger.timer "
            "cratedigger-web.service cratedigger-importer.service "
            "cratedigger-import-preview-worker.service "
            "cratedigger-youtube-ingest.service)",
        )


class TestFakeGateHoldModelsTheRealGuardedSet(unittest.TestCase):
    """#1100 item 1, closed against the real fault: FakeDeployHoldBackend's
    ``metadata_gate("hold manual")`` must stop every ``GATE_GUARDED_UNITS``
    member the existing acquire/recover/prepare_controlled suite does not
    already prove on its own.

    The four always-on daemons (web, preview, importer, YouTube) default to
    active in this fake (``_ALWAYS_ON_DAEMONS`` -- the real world every
    acquire meets), so dropping any of them from the guarded loop is already
    self-caught: they would stay active past "hold manual," and the
    post-hold ``SERVICE_UNITS`` drain that follows in every existing
    acquire/recover test would then time out waiting for them, failing that
    test on its own. ``cratedigger.timer`` and ``cratedigger.service`` are
    the two members that suite does NOT cover: every current production
    call path masks and stops the timers before ever taking the gate hold,
    and this fake defaults ``cratedigger.service`` to inactive at
    construction, so neither is ever active at "hold manual" time through
    any real call chain -- two independently planted mutants, each dropping
    one of these two units from the fake's guarded loop while keeping the
    other, each survived every one of those tests unchanged. Driving the
    fake's own method directly, with both forced active first, is what
    makes either omission observable.
    """

    def test_hold_manual_stops_an_active_main_timer_and_service(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.unit_states[MAIN_TIMER] = UnitState(
            load_state="loaded", active_state="active", sub_state="waiting",
        )
        backend.unit_states[MAIN_SERVICE] = UnitState(
            load_state="loaded", active_state="active", sub_state="running",
        )

        backend.metadata_gate("hold manual")

        self.assertEqual(
            backend.unit_state(MAIN_TIMER),
            UnitState(load_state="loaded", active_state="inactive", sub_state="dead"),
        )
        self.assertEqual(
            backend.unit_state(MAIN_SERVICE),
            UnitState(load_state="loaded", active_state="inactive", sub_state="dead"),
        )


class TestRecoverHeldWaitsOutAnActiveTimerDrivenProducer(unittest.TestCase):
    """#1100 item 2.

    recover_held's non-acquiring else branch (any phase from PHASE_HELD
    onward) re-establishes the manual gate hold with NO producer drain
    before it -- unlike acquire_hold's acquiring branch, which always drains
    TIMER_DRIVEN_PRODUCER_UNITS first (_drain_producers_then_hold). Production
    is still correct there: cratedigger-unfindable.service and the watchdog
    are bounded oneshots the gate does not guard (GATE_GUARDED_UNITS), so the
    post-hold SERVICE_UNITS drain that already runs unconditionally is what
    waits a still-running one out. Nothing pinned that before this.
    """

    def test_recover_held_from_prepared_controlled_waits_out_a_running_unfindable_cycle(
        self,
    ) -> None:
        # UNFINDABLE_SERVICE is never gate-guarded -- the gate hold literally
        # cannot reap it, so only the drain below can.
        self.assertNotIn(UNFINDABLE_SERVICE, GATE_GUARDED_UNITS)

        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        prepare_controlled(backend)
        self.assertEqual(backend.phase, PHASE_PREPARED_CONTROLLED)
        # prepare_controlled released the manual hold -- recover_held's else
        # branch is about to re-take it.
        self.assertFalse(backend.manual_hold)

        # A genuinely active timer-driven producer at the exact moment this
        # recovery is invoked -- NOT a "timer fired right before its own
        # mask took effect" story: that is impossible this late.
        # prepare_controlled's own _verify_authoritative_hold already
        # re-proved every timer masked and every SERVICE_UNITS member
        # stably inactive before PHASE_PREPARED_CONTROLLED was ever reached.
        # The producible route is a manual `systemctl start
        # cratedigger-unfindable.service` (or the watchdog) during the
        # deploy window: masking a timer blocks only that timer's own
        # trigger, never a direct start of the service it drives, and
        # neither service carries a START_INHIBITORS entry the way main and
        # YouTube do. _drain_producers_then_hold's own docstring documents
        # the identical mechanism -- a timer mask blocking only its own
        # trigger, not a manual `systemctl start` of the service -- for
        # cratedigger.service in that function's own pre-hold window.
        backend.unit_states[UNFINDABLE_SERVICE] = UnitState(
            load_state="loaded", active_state="active", sub_state="running",
        )
        backend.running_samples[UNFINDABLE_SERVICE] = 3
        sleep_calls_before = backend.sleep_calls

        recover_held(backend)

        backend.assert_default_held()
        # The drain waited it out. Measured mechanism for this 3-tick
        # countdown: 3 sleeps decrement running_samples to 0, a 4th sleep is
        # the one that actually reaps the unit to inactive/dead once the
        # countdown is exhausted, and a 5th intervening sleep is needed to
        # reach the two-consecutive-stable-samples requirement (5 total).
        # The assertion only requires >= 3 -- comfortably below that
        # measured total, so it stays robust to exact-count drift in the
        # fake's stability bookkeeping.
        self.assertGreaterEqual(backend.sleep_calls - sleep_calls_before, 3)


class TestFixedAuthoritySurface(unittest.TestCase):
    def test_only_system_control_timer_paths_can_be_owned(self) -> None:
        self.assertEqual(
            str(METADATA_MANUAL_HOLD),
            "/var/lib/cratedigger-metadata-gate/holds/manual",
        )
        for timer in TIMER_UNITS:
            self.assertTrue(timer.endswith(".timer"))
            self.assertEqual(f"{CONTROL_DIR}/{timer}".split("/")[-1], timer)
        for service in SERVICE_UNITS:
            self.assertTrue(service.endswith(".service"))
            self.assertNotIn(service, TIMER_UNITS)
        self.assertEqual(set(START_INHIBITORS), {
            MAIN_SERVICE,
            YOUTUBE_SERVICE,
        })

    def test_deploy_skill_uses_tracked_hold_and_cycle_boundaries(self) -> None:
        helper = REPO_ROOT / "scripts" / "cratedigger_deploy_hold.py"
        skill = REPO_ROOT / ".claude" / "skills" / "deploy" / "SKILL.md"
        helper_source = helper.read_text(encoding="utf-8")
        skill_source = pinned_source(skill)

        self.assertEqual(helper_source.splitlines()[0], "#!/usr/bin/env python3")
        self.assertIn("cratedigger_deploy_hold.py", skill_source)
        self.assertIn("verify_cratedigger_cycle.sh", skill_source)
        self.assertIn("CONTROLLED_CURSOR=$(\"$CYCLE_VERIFY\" capture-cursor)", skill_source)
        self.assertIn("ORDINARY_CURSOR=$(\"$CYCLE_VERIFY\" capture-cursor)", skill_source)
        self.assertNotIn("CONTROLLED_PREVIOUS", skill_source)
        self.assertNotIn("ORDINARY_PREVIOUS", skill_source)
        strict_hold = skill_source.split(
            "## Holding timer-driven work across a switch", 1
        )[1].split("## Database migrations", 1)[0]
        self.assertNotRegex(
            strict_hold,
            r"(?m)^\s*(?:sudo\s+)?systemctl\s+mask\b",
        )


class TestRealSystemdBackendMarkerLifecycle(unittest.TestCase):
    """#1096 correction round, S2: a tmpdir-backed RealSystemdBackend proves
    the REAL filesystem marker write/remove logic -- not merely the fake's
    event-log model of it -- creates and then deletes the real persistent
    sibling marker file. Nothing else in the Python suite drives
    RealSystemdBackend's own marker file operations at all; before this,
    only the VM leg did, so a mutant dropping the persistent-marker removal
    call from ``unmark_inhibitor_owned`` / ``unmark_manual_hold_owned``
    passed the entire Python suite.

    Every ownership check in ``RealSystemdBackend`` requires root
    ownership in production, compared against the module constant
    ``_ROOT_UID`` rather than a bare literal -- patched here to the test
    process's own uid so the SAME validation logic runs against a real
    tmpdir without requiring the test runner itself to run as root.
    """

    def _make_backend(self) -> tuple[RealSystemdBackend, Path]:
        tmp = Path(self.enterContext(tempfile.TemporaryDirectory()))
        state_dir = tmp / "deploy-hold"
        metadata_gate_dir = tmp / "metadata-gate"
        state_dir.mkdir()
        os.chmod(state_dir, 0o700)
        metadata_gate_dir.mkdir()
        os.chmod(metadata_gate_dir, 0o700)
        self.enterContext(
            mock.patch.object(deploy_hold_module, "STATE_DIR", state_dir)
        )
        self.enterContext(
            mock.patch.object(
                deploy_hold_module, "METADATA_GATE_STATE_DIR", metadata_gate_dir,
            )
        )
        self.enterContext(
            mock.patch.object(deploy_hold_module, "_ROOT_UID", os.getuid())
        )
        return RealSystemdBackend(), metadata_gate_dir

    def test_mark_and_unmark_manual_hold_round_trips_the_real_persistent_marker(
        self,
    ) -> None:
        backend, metadata_gate_dir = self._make_backend()
        marker_path = metadata_gate_dir / "deploy-hold-owned-manual"
        self.assertFalse(marker_path.exists())

        backend.mark_manual_hold_owned()

        self.assertTrue(marker_path.is_file())
        self.assertEqual(marker_path.read_text(encoding="utf-8"), "manual\n")
        self.assertEqual(stat.S_IMODE(marker_path.stat().st_mode), 0o600)
        self.assertTrue(backend.persistent_manual_marker_exists())

        backend.unmark_manual_hold_owned()

        self.assertFalse(marker_path.exists())
        self.assertFalse(backend.persistent_manual_marker_exists())

    def test_mark_and_unmark_inhibitor_round_trips_the_real_persistent_marker(
        self,
    ) -> None:
        backend, metadata_gate_dir = self._make_backend()
        marker_path = (
            metadata_gate_dir / f"deploy-hold-owned-inhibit-{YOUTUBE_SERVICE}"
        )
        self.assertFalse(marker_path.exists())

        backend.mark_inhibitor_owned(YOUTUBE_SERVICE)

        self.assertTrue(marker_path.is_file())
        self.assertEqual(
            marker_path.read_text(encoding="utf-8"), f"{YOUTUBE_SERVICE}\n",
        )
        self.assertEqual(stat.S_IMODE(marker_path.stat().st_mode), 0o600)
        self.assertTrue(backend.persistent_inhibitor_marker_exists(YOUTUBE_SERVICE))

        backend.unmark_inhibitor_owned(YOUTUBE_SERVICE)

        self.assertFalse(marker_path.exists())
        self.assertFalse(backend.persistent_inhibitor_marker_exists(YOUTUBE_SERVICE))

    def test_mark_manual_hold_owned_persists_the_marker_before_the_tmpfs_marker_on_the_real_backend(
        self,
    ) -> None:
        """#1096 correction round, S2: the fake-driven ordering pins in
        TestReceiptlessAbortAdoptsPersistentMarkers only prove ordering
        through the fake's own event list -- nothing in the rest of the
        suite drives RealSystemdBackend's own two `os.open` calls, so a
        mutant swapping their order inside `mark_manual_hold_owned` passed
        the entire Python suite. Spies on the real `os.open` calls
        (patching the same `os` module `mark_manual_hold_owned` itself
        imports and calls through) to observe the ACTUAL write order.
        """
        backend, metadata_gate_dir = self._make_backend()
        persistent_marker_path = metadata_gate_dir / "deploy-hold-owned-manual"
        real_open = os.open
        opened_paths: list[str] = []

        def spying_open(
            path: str | os.PathLike[str], flags: int, mode: int = 0o777
        ) -> int:
            opened_paths.append(os.fspath(path))
            return real_open(path, flags, mode)

        with mock.patch.object(deploy_hold_module.os, "open", spying_open):
            backend.mark_manual_hold_owned()

        persistent_index = opened_paths.index(str(persistent_marker_path))
        tmpfs_marker_index = next(
            index
            for index, path in enumerate(opened_paths)
            if path.endswith("/owned-manual-hold")
        )
        self.assertLess(
            persistent_index,
            tmpfs_marker_index,
            f"persistent marker was not opened before the tmpfs marker: "
            f"{opened_paths!r}",
        )

    def test_mark_inhibitor_owned_persists_the_marker_before_the_tmpfs_marker_on_the_real_backend(
        self,
    ) -> None:
        """#1096 correction round, S2 (inhibitor mirror of the manual-hold
        real-backend ordering test above)."""
        backend, metadata_gate_dir = self._make_backend()
        persistent_marker_path = (
            metadata_gate_dir / f"deploy-hold-owned-inhibit-{YOUTUBE_SERVICE}"
        )
        real_open = os.open
        opened_paths: list[str] = []

        def spying_open(
            path: str | os.PathLike[str], flags: int, mode: int = 0o777
        ) -> int:
            opened_paths.append(os.fspath(path))
            return real_open(path, flags, mode)

        with mock.patch.object(deploy_hold_module.os, "open", spying_open):
            backend.mark_inhibitor_owned(YOUTUBE_SERVICE)

        persistent_index = opened_paths.index(str(persistent_marker_path))
        tmpfs_marker_index = next(
            index
            for index, path in enumerate(opened_paths)
            if path.endswith(f"/owned-inhibitor-{YOUTUBE_SERVICE}")
        )
        self.assertLess(
            persistent_index,
            tmpfs_marker_index,
            f"persistent marker was not opened before the tmpfs marker: "
            f"{opened_paths!r}",
        )


if __name__ == "__main__":
    unittest.main()
