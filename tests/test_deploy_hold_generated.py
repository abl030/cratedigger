"""Generated lifecycle contracts for the authoritative deployment hold."""

from __future__ import annotations

import unittest
from itertools import product

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from scripts.cratedigger_deploy_hold import (
    CONTROLLED_WORKER_UNITS,
    MAIN_SERVICE,
    MAIN_TIMER,
    PHASE_ACQUIRING,
    PHASE_COMPLETE_PENDING,
    PHASE_HELD,
    SERVICE_UNITS,
    TIMER_UNITS,
    YOUTUBE_SERVICE,
    DeployHoldError,
    JobState,
    LifecyclePreflight,
    acquire_hold,
    complete_release,
    finish_release,
    open_main_timer,
    prepare_controlled,
    recover_held,
)
from tests.fakes.deploy_hold import FakeDeployHoldBackend


def assert_held_invariants(backend: FakeDeployHoldBackend) -> None:
    if backend.phase != PHASE_HELD:
        raise AssertionError(f"hold phase is {backend.phase!r}")
    if not backend.manual_hold or not backend.owned_manual_hold:
        raise AssertionError("manual metadata hold is not owned and active")
    if backend.owned_links != set(TIMER_UNITS):
        raise AssertionError(f"wrong owned timer set: {backend.owned_links!r}")
    for timer in TIMER_UNITS:
        if backend.control_links.get(timer) != "/dev/null":
            raise AssertionError(f"timer {timer} lacks authoritative mask")
        if backend.unit_state(timer).load_state != "masked":
            raise AssertionError(f"timer {timer} is not LoadState=masked")
    for service in SERVICE_UNITS:
        state = backend.unit_state(service)
        if (state.active_state, state.sub_state) != ("inactive", "dead"):
            raise AssertionError(f"service {service} is not stably inactive")
        if backend.job_state(service) != JobState.none():
            raise AssertionError(f"service {service} still has a job")
        if service in backend.control_links:
            raise AssertionError(f"service {service} was masked")
    if backend.owned_inhibitors or backend.inhibitor_files:
        raise AssertionError("strict hold retained a producer inhibitor")


def assert_release_invariants(
    backend: FakeDeployHoldBackend,
    invocation_id: str,
) -> None:
    if backend.receipt:
        raise AssertionError("completed release retained its receipt")
    if backend.manual_hold or backend.owned_manual_hold:
        raise AssertionError("completed release retained the manual hold")
    if backend.control_links or backend.owned_links:
        raise AssertionError("completed release retained a control link")
    if backend.inhibitor_files or backend.owned_inhibitors:
        raise AssertionError("completed release retained a producer inhibitor")
    for timer in TIMER_UNITS:
        state = backend.unit_state(timer)
        if state.load_state != "loaded" or state.active_state != "active":
            raise AssertionError(f"timer {timer} was not restored")
    completed = [event for event in backend.events if event == ("receipt-remove",)]
    if len(completed) != 1:
        raise AssertionError("receipt was not cleared exactly once")
    if ("ordinary-invocation", invocation_id) not in backend.events:
        raise AssertionError("ordinary successor identity was not retained")


def assert_no_unproven_promotion(
    backend: FakeDeployHoldBackend,
    *,
    phase_before: str | None,
    preconditions_provable: bool,
) -> None:
    """Only a receipt whose acquire preconditions hold may reach HELD.

    ``create_receipt`` persists ``acquiring`` before ``acquire_hold`` reaches
    the controlled-start contract or the old-lifecycle preflight, so a receipt
    still in that phase has never proven either. Recovery may not promote it to
    the boundary ``verify_held``/``prepare_controlled`` trust.
    """
    if (
        phase_before == PHASE_ACQUIRING
        and not preconditions_provable
        and backend.phase == PHASE_HELD
    ):
        raise AssertionError(
            "recovery promoted an unproven acquiring receipt to the held "
            "boundary"
        )


SERVICE_CONDITION = st.sampled_from(("none", "waiting", "running", "failed"))


@st.composite
def job_worlds(
    draw: st.DrawFn,
) -> tuple[dict[str, JobState], dict[str, int], set[str]]:
    jobs: dict[str, JobState] = {}
    running_samples: dict[str, int] = {}
    failed_services: set[str] = set()
    for index, service in enumerate(SERVICE_UNITS, start=1):
        kind = draw(SERVICE_CONDITION)
        if kind == "none":
            continue
        if kind == "failed":
            failed_services.add(service)
            continue
        jobs[service] = JobState(
            job_id=str(100 + index),
            unit=service,
            job_type="start",
            state=kind,
        )
        if kind == "running":
            running_samples[service] = draw(st.integers(min_value=0, max_value=4))
    return jobs, running_samples, failed_services


class TestGeneratedHoldLifecycle(unittest.TestCase):
    @given(
        counts=st.tuples(
            st.integers(min_value=0, max_value=3),
            st.integers(min_value=0, max_value=3),
            st.integers(min_value=0, max_value=3),
            st.integers(min_value=0, max_value=3),
        ).filter(lambda values: any(values)),
    )
    @example(counts=(1, 0, 0, 0))
    @example(counts=(0, 1, 0, 0))
    @example(counts=(0, 0, 1, 0))
    @example(counts=(0, 0, 0, 1))
    def test_any_dirty_old_lifecycle_shape_aborts_under_the_hold(
        self,
        counts: tuple[int, int, int, int],
    ) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(*counts),
        )

        with self.assertRaisesRegex(
            DeployHoldError,
            "old lifecycle is not clean",
        ):
            acquire_hold(backend)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        self.assertTrue(backend.manual_hold)
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        self.assertEqual(backend.inhibitor_files, set())

    def test_atomic_receipt_publication_retry_precedes_hold_mutation(self) -> None:
        for interrupt_publication in (False, True):
            with self.subTest(interrupt_publication=interrupt_publication):
                backend = FakeDeployHoldBackend(
                    interrupt_receipt_publication=interrupt_publication,
                )
                if interrupt_publication:
                    with self.assertRaises(InterruptedError):
                        acquire_hold(backend)
                    self.assertFalse(backend.receipt)
                    self.assertFalse(backend.manual_hold)
                    self.assertEqual(backend.control_links, {})

                acquire_hold(backend)
                assert_held_invariants(backend)

    def test_any_incomplete_release_phase_can_recover_to_strict_hold(self) -> None:
        # The four numbered phases are the complete finite recovery vocabulary;
        # phase 3 preserves the former decisive @example.
        for release_phase in range(4):
            with self.subTest(release_phase=release_phase):
                backend = FakeDeployHoldBackend()
                acquire_hold(backend)
                if release_phase >= 1:
                    prepare_controlled(backend)
                if release_phase >= 2:
                    open_main_timer(backend)
                if release_phase >= 3:
                    finish_release(backend, "a" * 32)

                recover_held(backend)

                assert_held_invariants(backend)
                self.assertIsNone(backend.ordinary_invocation)

    @given(
        acquire_counts=st.tuples(
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
        ),
        recovery_counts=st.tuples(
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
            st.integers(min_value=0, max_value=2),
        ),
        recovery_contract_current=st.booleans(),
        release_phase=st.integers(min_value=0, max_value=3),
    )
    # A receipt produced by a FAILED acquire, still dirty at recovery time.
    @example(
        acquire_counts=(1, 0, 0, 0),
        recovery_counts=(1, 0, 0, 0),
        recovery_contract_current=True,
        release_phase=0,
    )
    # The same unproven receipt, with a stale downstream start contract.
    @example(
        acquire_counts=(0, 0, 0, 1),
        recovery_counts=(0, 0, 0, 0),
        recovery_contract_current=False,
        release_phase=0,
    )
    # An unproven receipt that became provable: recovery must still succeed.
    @example(
        acquire_counts=(0, 1, 0, 0),
        recovery_counts=(0, 0, 0, 0),
        recovery_contract_current=True,
        release_phase=0,
    )
    # A proven receipt whose post-migration world can no longer satisfy the
    # pre-migration proof: recovery must not be bricked by it.
    @example(
        acquire_counts=(0, 0, 0, 0),
        recovery_counts=(2, 2, 2, 2),
        recovery_contract_current=False,
        release_phase=1,
    )
    def test_recovery_reaches_held_only_from_a_provable_receipt(
        self,
        acquire_counts: tuple[int, int, int, int],
        recovery_counts: tuple[int, int, int, int],
        recovery_contract_current: bool,
        release_phase: int,
    ) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(*acquire_counts),
        )
        if any(acquire_counts):
            with self.assertRaises(DeployHoldError):
                acquire_hold(backend)
        else:
            acquire_hold(backend)
            if release_phase >= 1:
                prepare_controlled(backend)
            if release_phase >= 2:
                open_main_timer(backend)
            if release_phase >= 3:
                finish_release(backend, "a" * 32)
        phase_before = backend.phase

        # The live world recovery observes, which the receipt's own phase does
        # not control: the lifecycle moves on and units are redeployed.
        backend.preflight = LifecyclePreflight(*recovery_counts)
        backend.controlled_start_contract_current = recovery_contract_current
        preconditions_provable = recovery_contract_current and not any(
            recovery_counts
        )

        if phase_before == PHASE_ACQUIRING and not preconditions_provable:
            with self.assertRaises(DeployHoldError):
                recover_held(backend)
            self.assertEqual(backend.phase, PHASE_ACQUIRING)
            # Refusing to promote never costs the strict boundary itself.
            self.assertTrue(backend.manual_hold)
            self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        else:
            recover_held(backend)
            assert_held_invariants(backend)
            self.assertIsNone(backend.ordinary_invocation)

        assert_no_unproven_promotion(
            backend,
            phase_before=phase_before,
            preconditions_provable=preconditions_provable,
        )

    def test_interrupted_acquisition_resumes_only_receipt_owned_intents(
        self,
    ) -> None:
        states = ("absent", "intent", "materialized")
        # Exhaustive Cartesian table over every timer link and the manual hold.
        # It includes the former materialized/intent/absent + intent example.
        for world in product(states, repeat=len(TIMER_UNITS) + 1):
            link_states = world[:-1]
            manual_state = world[-1]
            with self.subTest(
                link_states=link_states,
                manual_state=manual_state,
            ):
                backend = FakeDeployHoldBackend()
                backend.create_receipt()
                for timer, state in zip(TIMER_UNITS, link_states, strict=True):
                    if state in {"intent", "materialized"}:
                        backend.mark_link_owned(timer)
                    if state == "materialized":
                        backend.create_control_mask(timer)
                if manual_state in {"intent", "active"}:
                    backend.mark_manual_hold_owned()
                if manual_state == "active":
                    backend.manual_hold = True

                acquire_hold(backend)

                assert_held_invariants(backend)

    @given(world=job_worlds())
    @example(
        world=(
            {
                MAIN_SERVICE: JobState(
                    job_id="101",
                    unit=MAIN_SERVICE,
                    job_type="start",
                    state="waiting",
                ),
                SERVICE_UNITS[1]: JobState(
                    job_id="102",
                    unit=SERVICE_UNITS[1],
                    job_type="start",
                    state="running",
                ),
            },
            {SERVICE_UNITS[1]: 2},
            {SERVICE_UNITS[2]},
        )
    )
    def test_acquire_cancels_only_waiting_starts_and_reaches_stable_hold(
        self,
        world: tuple[dict[str, JobState], dict[str, int], set[str]],
    ) -> None:
        jobs, running_samples, failed_services = world
        backend = FakeDeployHoldBackend(
            jobs=jobs,
            running_samples=running_samples,
            failed_services=failed_services,
        )

        acquire_hold(backend)

        assert_held_invariants(backend)
        expected_cancelled = sorted(
            job.job_id for job in jobs.values() if job.state == "waiting"
        )
        self.assertEqual(sorted(backend.cancelled_jobs), expected_cancelled)
        self.assertEqual(
            sorted(
                event[1]
                for event in backend.events
                if event[0] == "reset-failed"
            ),
            sorted(failed_services),
        )

    @given(
        invocation_id=st.from_regex(r"[0-9a-f]{32}", fullmatch=True),
        interrupt_retirement=st.booleans(),
    )
    @example(
        invocation_id="7d4bd1dbb52e4a2ba2f314fee90f8989",
        interrupt_retirement=True,
    )
    def test_full_release_restores_exact_timers_and_clears_receipt(
        self,
        invocation_id: str,
        interrupt_retirement: bool,
    ) -> None:
        backend = FakeDeployHoldBackend(
            interrupt_receipt_retirement=interrupt_retirement,
        )
        acquire_hold(backend)
        prepare_controlled(backend)
        open_main_timer(backend)
        finish_release(backend, invocation_id)
        self.assertEqual(backend.phase, PHASE_COMPLETE_PENDING)
        if interrupt_retirement:
            with self.assertRaises(InterruptedError):
                complete_release(backend, invocation_id)
        complete_release(backend, invocation_id)

        assert_release_invariants(backend, invocation_id)
        self.assertEqual(
            backend.started_units,
            [*CONTROLLED_WORKER_UNITS, MAIN_SERVICE, *TIMER_UNITS],
        )


class TestHoldInvariantCheckersKnownBad(unittest.TestCase):
    def test_held_checker_rejects_low_precedence_or_service_mask(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        del backend.control_links[MAIN_TIMER]
        backend.control_links[MAIN_SERVICE] = "/dev/null"

        with self.assertRaises(AssertionError):
            assert_held_invariants(backend)

    def test_held_checker_rejects_surviving_queued_start(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        backend.jobs[MAIN_SERVICE] = JobState(
            job_id="999",
            unit=MAIN_SERVICE,
            job_type="start",
            state="waiting",
        )

        with self.assertRaises(AssertionError):
            assert_held_invariants(backend)

    def test_release_checker_rejects_retained_owned_link(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.receipt = True
        backend.phase = PHASE_COMPLETE_PENDING
        backend.control_links[MAIN_TIMER] = "/dev/null"
        backend.owned_links.add(MAIN_TIMER)

        with self.assertRaises(AssertionError):
            assert_release_invariants(backend, "a" * 32)

    def test_held_checker_rejects_a_retained_producer_inhibitor(self) -> None:
        backend = FakeDeployHoldBackend()
        acquire_hold(backend)
        backend.inhibitor_files.add(YOUTUBE_SERVICE)

        with self.assertRaises(AssertionError):
            assert_held_invariants(backend)

    def test_promotion_checker_trips_on_a_promoted_unproven_receipt(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
        )
        with self.assertRaises(DeployHoldError):
            acquire_hold(backend)
        self.assertEqual(backend.phase, PHASE_ACQUIRING)
        # Plant exactly the defect: promote the never-proven receipt.
        backend.write_phase(PHASE_HELD)

        with self.assertRaises(AssertionError):
            assert_no_unproven_promotion(
                backend,
                phase_before=PHASE_ACQUIRING,
                preconditions_provable=False,
            )

    def test_promotion_checker_accepts_a_reproven_acquiring_receipt(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(1, 0, 0, 0),
        )
        with self.assertRaises(DeployHoldError):
            acquire_hold(backend)
        backend.preflight = LifecyclePreflight(0, 0, 0, 0)

        recover_held(backend)

        assert_no_unproven_promotion(
            backend,
            phase_before=PHASE_ACQUIRING,
            preconditions_provable=True,
        )
        assert_held_invariants(backend)

    def test_release_checker_rejects_a_retained_owned_inhibitor(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.inhibitor_files.add(YOUTUBE_SERVICE)
        backend.owned_inhibitors.add(YOUTUBE_SERVICE)

        with self.assertRaises(AssertionError):
            assert_release_invariants(backend, "a" * 32)


if __name__ == "__main__":
    unittest.main()
