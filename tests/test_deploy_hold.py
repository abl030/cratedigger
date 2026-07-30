"""Deterministic contracts for the authoritative deployment hold."""

from __future__ import annotations

import os
import sys
import unittest
from collections.abc import Mapping
from pathlib import Path

import psycopg2
import psycopg2.extras

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401 - starts and migrates isolated PostgreSQL

from scripts.cratedigger_deploy_hold import (
    CONTROL_DIR,
    CONTROLLED_WORKER_UNITS,
    MAIN_SERVICE,
    MAIN_TIMER,
    METADATA_MANUAL_HOLD,
    PHASE_ACQUIRING,
    PHASE_COMPLETE_PENDING,
    PHASE_HELD,
    PHASE_MAIN_TIMER_OPEN,
    PHASE_PREPARED_CONTROLLED,
    SERVICE_UNITS,
    START_INHIBITORS,
    TIMER_UNITS,
    YOUTUBE_SERVICE,
    DeployHoldError,
    JobState,
    LifecyclePreflight,
    RealSystemdBackend,
    UnitState,
    acquire_hold,
    complete_release,
    finish_release,
    open_main_timer,
    prepare_controlled,
    recover_held,
    verify_held,
)
from scripts.pipeline_cli.query import _render_query_table
from tests.fakes.deploy_hold import FakeDeployHoldBackend

INVOCATION = "a" * 32
REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_DSN = os.environ["TEST_DB_DSN"]


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

    def test_dirty_old_lifecycle_stays_authoritatively_acquiring(self) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
        )

        with self.assertRaisesRegex(
            DeployHoldError,
            "active_automation_jobs",
        ):
            acquire_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        self.assertTrue(backend.manual_hold)
        self.assertEqual(backend.control_links, {
            timer: "/dev/null" for timer in TIMER_UNITS
        })


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

    def test_invocation_id_must_be_exact_systemd_shape(self) -> None:
        prepare_controlled(self.backend)
        open_main_timer(self.backend)

        for invalid in ("", "none", "xyz", "a" * 31, "a" * 33):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(DeployHoldError, "InvocationID"):
                finish_release(self.backend, invalid)


class TestRecoveryReprovesAnUnprovenAcquisition(unittest.TestCase):
    """A receipt reaches HELD only through the acquire preconditions."""

    def test_recovery_from_acquiring_refuses_a_dirty_old_lifecycle(self) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(0, 0, 2, 0),
        )
        with self.assertRaisesRegex(
            DeployHoldError,
            "dirty_downloading_rows",
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
        skill_source = skill.read_text(encoding="utf-8")

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


if __name__ == "__main__":
    unittest.main()
