"""Deterministic contracts for the authoritative deployment hold."""

from __future__ import annotations

from pathlib import Path
import unittest

from scripts.cratedigger_deploy_hold import (
    CONTROL_DIR,
    MAIN_SERVICE,
    MAIN_TIMER,
    PHASE_COMPLETE_PENDING,
    PHASE_HELD,
    PHASE_MAIN_TIMER_OPEN,
    PHASE_PREPARED_CONTROLLED,
    SERVICE_UNITS,
    TIMER_UNITS,
    DeployHoldError,
    JobState,
    UnitState,
    acquire_hold,
    complete_release,
    finish_release,
    open_main_timer,
    prepare_controlled,
    recover_held,
    verify_held,
)
from tests.fakes.deploy_hold import FakeDeployHoldBackend


INVOCATION = "a" * 32
REPO_ROOT = Path(__file__).resolve().parent.parent


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
        self.assertEqual(self.backend.started_units, [MAIN_SERVICE])
        self.assertEqual(self.backend.owned_links, set(TIMER_UNITS))

        # PR1 verifies the controlled invocation before this transition.
        open_main_timer(self.backend)
        self.assertEqual(self.backend.phase, PHASE_MAIN_TIMER_OPEN)
        self.assertNotIn(MAIN_TIMER, self.backend.owned_links)
        self.assertNotIn(MAIN_TIMER, self.backend.control_links)
        self.assertEqual(self.backend.unit_state(MAIN_TIMER).load_state, "loaded")
        self.assertEqual(self.backend.started_units, [MAIN_SERVICE, MAIN_TIMER])
        for timer in TIMER_UNITS:
            if timer != MAIN_TIMER:
                self.assertEqual(self.backend.unit_state(timer).load_state, "masked")

        # PR1 capture-target returns this ID before the ordinary cycle finishes.
        finish_release(self.backend, INVOCATION)
        self.assertEqual(self.backend.phase, PHASE_COMPLETE_PENDING)
        self.assertEqual(self.backend.ordinary_invocation, INVOCATION)
        self.assertEqual(self.backend.owned_links, set())
        self.assertEqual(self.backend.control_links, {})
        self.assertIn(("metadata-gate", "resume-if-clear"), self.backend.events)
        self.assertEqual(
            self.backend.started_units,
            [MAIN_SERVICE, *TIMER_UNITS],
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
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(DeployHoldError, "InvocationID"):
                    finish_release(self.backend, invalid)


class TestFixedAuthoritySurface(unittest.TestCase):
    def test_only_system_control_timer_paths_can_be_owned(self) -> None:
        for timer in TIMER_UNITS:
            self.assertTrue(timer.endswith(".timer"))
            self.assertEqual(f"{CONTROL_DIR}/{timer}".split("/")[-1], timer)
        for service in SERVICE_UNITS:
            self.assertTrue(service.endswith(".service"))
            self.assertNotIn(service, TIMER_UNITS)

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
