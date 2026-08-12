"""Generated lifecycle contracts for the authoritative deployment hold."""

from __future__ import annotations

import unittest
from itertools import product

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from scripts.cratedigger_deploy_hold import (
    CONTROLLED_WORKER_UNITS,
    IMPORTER_SERVICE,
    MAIN_SERVICE,
    MAIN_TIMER,
    PHASE_ACQUIRING,
    PHASE_COMPLETE_PENDING,
    PHASE_HELD,
    PHASE_MAIN_TIMER_OPEN,
    PHASE_PREPARED_CONTROLLED,
    SERVICE_UNITS,
    TIMER_UNITS,
    WATCHDOG_TIMER,
    YOUTUBE_SERVICE,
    DeployHoldBackend,
    DeployHoldError,
    JobState,
    LifecyclePreflight,
    UnitState,
    _clear_owned_inhibitors,
    _release_owned_link,
    abort_hold,
    acquire_hold,
    complete_release,
    finish_release,
    open_main_timer,
    prepare_controlled,
    recover_held,
)
from tests.fakes.deploy_hold import FakeDeployHoldBackend

_KNOWN_PHASES = (
    PHASE_ACQUIRING,
    PHASE_HELD,
    PHASE_PREPARED_CONTROLLED,
    PHASE_MAIN_TIMER_OPEN,
    PHASE_COMPLETE_PENDING,
)
# No production-constant patch needed: the fake's monotonic clock advances by
# the real requested duration per sleep() call (tests/fakes/deploy_hold.py),
# and its sleep is instant -- so even the unpatched production
# _QUEUE_DRAIN_TIMEOUT_SECONDS/_QUEUE_POLL_SECONDS bound (1800s / 5s poll =
# 360 loop iterations) is fast, and every property here exercises the exact
# bound production uses.


def _acquire_boundary_outcome(
    counts: tuple[int, int, int, int],
) -> str:
    """Classify what acquire_hold/recover_held(acquiring) does against this
    preflight, given nothing configured to resolve the queue over time
    (``queue_drain_after_calls=None``).

    Returns ``"held"`` (everything clean), ``"anomaly"`` (fails immediately
    at the final old-lifecycle check, gate hold taken), or
    ``"drainable_timeout"`` (bounded wait, gate hold never taken).

    ``recovery_required_jobs`` is ALSO counted inside
    ``active_automation_jobs``'s own SQL (``status IN ('queued', 'running',
    'recovery_required')``), and neither anomaly field
    (``recovery_required_jobs``, ``malformed_enqueued_at_rows``) is ever
    cleared by any current writer -- so an anomaly field dirty takes
    precedence over a drainable one: the queue-drain wait short-circuits
    immediately rather than waiting toward its own timeout for a count that
    can never reach zero (#1078 MUST FIX 6). Field order matches
    ``LifecyclePreflight``: active_automation_jobs, recovery_required_jobs,
    dirty_downloading_rows, malformed_enqueued_at_rows.
    """
    (
        active_automation_jobs,
        recovery_required_jobs,
        dirty_downloading_rows,
        malformed_enqueued_at_rows,
    ) = counts
    if recovery_required_jobs or malformed_enqueued_at_rows:
        return "anomaly"
    if active_automation_jobs or dirty_downloading_rows:
        return "drainable_timeout"
    return "held"


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
    @example(counts=(1, 1, 0, 0))
    def test_any_dirty_old_lifecycle_shape_aborts_under_the_hold(
        self,
        counts: tuple[int, int, int, int],
    ) -> None:
        """#1078: a drainable field (alone) waits (bounded) with the gate
        hold never taken; an anomaly field -- alone, or combined with a
        drainable one, since it takes precedence (MUST FIX 6) -- fails
        immediately with the full boundary (masks + gate hold) established,
        same as before the reorder. The pre-hold window owns no start
        inhibitor at all (MUST FIX 5), so neither outcome ever owns one.
        """
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=LifecyclePreflight(*counts),
        )
        outcome = _acquire_boundary_outcome(counts)

        if outcome == "drainable_timeout":
            with self.assertRaisesRegex(
                DeployHoldError,
                "timed out waiting for the automation queue to drain",
            ):
                acquire_hold(backend)
            self.assertFalse(backend.manual_hold)
            self.assertFalse(backend.owned_manual_hold)
        else:
            assert outcome == "anomaly"
            with self.assertRaisesRegex(
                DeployHoldError,
                "old lifecycle is not clean",
            ):
                acquire_hold(backend)
            self.assertTrue(backend.manual_hold)

        self.assertTrue(backend.receipt)
        self.assertEqual(backend.phase, "acquiring")
        self.assertEqual(backend.owned_links, set(TIMER_UNITS))
        self.assertEqual(backend.owned_inhibitor_units(), ())
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
            # #1078: _ensure_owned_manual_hold only ever takes the gate, never
            # releases it -- so if an earlier pass (e.g. the acquire_counts
            # anomaly failure above) already took it, it stays taken
            # regardless of what this recovery's own preflight shows; a
            # drainable-only field (MUST FIX 6: an anomaly field takes
            # precedence) only keeps a hold NOT YET taken from being
            # (re)taken (bounded wait, gate never touched, exactly like a
            # fresh acquire).
            manual_hold_before_recovery = backend.manual_hold
            with self.assertRaises(DeployHoldError):
                recover_held(backend)
            self.assertEqual(backend.phase, PHASE_ACQUIRING)
            self.assertEqual(
                backend.manual_hold,
                manual_hold_before_recovery
                or _acquire_boundary_outcome(recovery_counts) != "drainable_timeout",
            )
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


def _assert_fully_reversed(backend: FakeDeployHoldBackend) -> None:
    """The #1078 invariant: abort leaves zero owned objects, no receipt."""
    if backend.owned_link_units():
        raise AssertionError(
            f"abort left owned timer links: {backend.owned_link_units()!r}"
        )
    if backend.manual_hold_is_owned():
        raise AssertionError("abort left the manual hold owned")
    if backend.owned_inhibitor_units():
        raise AssertionError(
            f"abort left owned inhibitors: {backend.owned_inhibitor_units()!r}"
        )
    if backend.receipt_exists():
        raise AssertionError("abort left the receipt in place")
    if backend.retired_receipt_exists():
        raise AssertionError("abort left a retired receipt behind")


def _assert_unowned_manual_hold_untouched(backend: FakeDeployHoldBackend) -> None:
    if not backend.manual_hold:
        raise AssertionError("abort released a manual hold it did not own")


def _assert_unowned_control_links_untouched(
    backend: FakeDeployHoldBackend,
    unowned_timers: frozenset[str],
) -> None:
    for timer in unowned_timers:
        if backend.control_links.get(timer) != "/dev/null":
            raise AssertionError(f"abort touched an unowned control link: {timer}")


def _abort_hold_ignores_manual_hold_ownership(backend: DeployHoldBackend) -> None:
    """Known-bad #1078 abort mutant.

    Releases the gate hold whether or not this receipt owns it -- the exact
    shape of bug the ownership guard in the real ``abort_hold`` exists to
    prevent. Retained only to prove the property traps it; never call this
    from production code.
    """
    if not backend.receipt_exists():
        raise DeployHoldError("deploy hold receipt is missing")
    backend.read_phase()
    if backend.manual_hold_active():
        backend.metadata_gate("release manual")
        for service in CONTROLLED_WORKER_UNITS:
            backend.start_unit(service)
    if backend.manual_hold_is_owned():
        backend.unmark_manual_hold_owned()
    _clear_owned_inhibitors(backend)
    owned_timers = backend.owned_link_units()
    for timer in owned_timers:
        _release_owned_link(backend, timer)
    if owned_timers:
        backend.daemon_reload()
        for timer in owned_timers:
            backend.start_unit(timer)
    backend.remove_receipt()


class TestFailedAcquireIsReversibleByAbort(unittest.TestCase):
    """#1078 property: every failed acquire_hold is fully reversed by abort_hold."""

    @given(
        active_automation_jobs=st.integers(min_value=0, max_value=2),
        dirty_downloading_rows=st.integers(min_value=0, max_value=2),
        recovery_required_jobs=st.integers(min_value=0, max_value=1),
        malformed_enqueued_at_rows=st.integers(min_value=0, max_value=1),
        queue_drain_after_calls=st.one_of(
            st.none(), st.integers(min_value=0, max_value=3),
        ),
        contract_current=st.booleans(),
    )
    @example(
        active_automation_jobs=1,
        dirty_downloading_rows=0,
        recovery_required_jobs=0,
        malformed_enqueued_at_rows=0,
        queue_drain_after_calls=1,
        contract_current=True,
    )
    @example(
        active_automation_jobs=0,
        dirty_downloading_rows=0,
        recovery_required_jobs=1,
        malformed_enqueued_at_rows=0,
        queue_drain_after_calls=None,
        contract_current=True,
    )
    @example(
        active_automation_jobs=1,
        dirty_downloading_rows=0,
        recovery_required_jobs=0,
        malformed_enqueued_at_rows=0,
        queue_drain_after_calls=None,
        contract_current=True,
    )
    @example(
        active_automation_jobs=0,
        dirty_downloading_rows=0,
        recovery_required_jobs=0,
        malformed_enqueued_at_rows=0,
        queue_drain_after_calls=None,
        contract_current=False,
    )
    def test_every_failed_acquire_is_fully_reversed_by_abort(
        self,
        active_automation_jobs: int,
        dirty_downloading_rows: int,
        recovery_required_jobs: int,
        malformed_enqueued_at_rows: int,
        queue_drain_after_calls: int | None,
        contract_current: bool,
    ) -> None:
        preflight = LifecyclePreflight(
            active_automation_jobs=active_automation_jobs,
            recovery_required_jobs=recovery_required_jobs,
            dirty_downloading_rows=dirty_downloading_rows,
            malformed_enqueued_at_rows=malformed_enqueued_at_rows,
        )
        backend = FakeDeployHoldBackend(
            lifecycle_preflight=preflight,
            controlled_start_contract_current=contract_current,
            queue_drain_after_calls=queue_drain_after_calls,
        )
        if queue_drain_after_calls is not None:
            # Models the causal claim behind the #1078 reorder: the
            # still-running importer is what drains the queue during the
            # wait window.
            backend.unit_states[IMPORTER_SERVICE] = UnitState(
                "loaded", "active", "running",
            )

        expected_success = (
            contract_current
            and recovery_required_jobs == 0
            and malformed_enqueued_at_rows == 0
            and (
                (active_automation_jobs == 0 and dirty_downloading_rows == 0)
                or queue_drain_after_calls is not None
            )
        )

        try:
            acquire_hold(backend)
            succeeded = True
        except DeployHoldError:
            succeeded = False

        self.assertEqual(succeeded, expected_success)
        if succeeded:
            backend.assert_default_held()
            return

        if not backend.receipt_exists():
            # The controlled-start contract failed before anything was ever
            # created -- nothing to abort, and abort says so plainly.
            with self.assertRaisesRegex(DeployHoldError, "receipt is missing"):
                abort_hold(backend)
            return

        abort_hold(backend)
        _assert_fully_reversed(backend)

    def test_known_bad_fully_reversed_checker_trips_on_an_owned_manual_hold(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.mark_manual_hold_owned()
        with self.assertRaisesRegex(AssertionError, "manual hold owned"):
            _assert_fully_reversed(backend)


class TestAbortNeverTouchesAnUnownedObject(unittest.TestCase):
    """#1078 property: abort_hold never mutates an object it did not own."""

    @given(
        owned_timers=st.frozensets(st.sampled_from(TIMER_UNITS)),
        unowned_timers=st.frozensets(st.sampled_from(TIMER_UNITS)),
        manual_hold_owned=st.booleans(),
        unowned_manual_hold=st.booleans(),
        phase=st.sampled_from(_KNOWN_PHASES),
    )
    @example(
        owned_timers=frozenset({MAIN_TIMER}),
        unowned_timers=frozenset({WATCHDOG_TIMER}),
        manual_hold_owned=False,
        unowned_manual_hold=False,
        phase=PHASE_ACQUIRING,
    )
    @example(
        owned_timers=frozenset(),
        unowned_timers=frozenset(),
        manual_hold_owned=False,
        unowned_manual_hold=True,
        phase=PHASE_ACQUIRING,
    )
    def test_abort_never_touches_an_unowned_object(
        self,
        owned_timers: frozenset[str],
        unowned_timers: frozenset[str],
        manual_hold_owned: bool,
        unowned_manual_hold: bool,
        phase: str,
    ) -> None:
        unowned_timers = unowned_timers - owned_timers
        if manual_hold_owned:
            # Not representable: there is exactly one gate, so it cannot be
            # simultaneously owned by this receipt and unowned-but-present.
            unowned_manual_hold = False

        backend = FakeDeployHoldBackend()
        backend.create_receipt()
        backend.write_phase(phase)
        for timer in owned_timers:
            backend.mark_link_owned(timer)
            backend.create_control_mask(timer)
        for timer in unowned_timers:
            backend.control_links[timer] = "/dev/null"
        if manual_hold_owned:
            backend.mark_manual_hold_owned()
            backend.manual_hold = True
        elif unowned_manual_hold:
            backend.manual_hold = True

        if unowned_timers:
            # #1078 MUST FIX 4: every ownership class is validated before
            # any mutation, so an unowned control link anywhere refuses
            # closed -- nothing moves, not even objects abort DOES own.
            before_control_links = dict(backend.control_links)
            before_owned_links = set(backend.owned_links)
            before_manual_hold = backend.manual_hold
            before_owned_manual_hold = backend.owned_manual_hold
            with self.assertRaisesRegex(DeployHoldError, "unowned control path"):
                abort_hold(backend)
            self.assertTrue(backend.receipt_exists())
            self.assertEqual(backend.control_links, before_control_links)
            self.assertEqual(backend.owned_links, before_owned_links)
            self.assertEqual(backend.manual_hold, before_manual_hold)
            self.assertEqual(backend.owned_manual_hold, before_owned_manual_hold)
            return

        abort_hold(backend)

        _assert_fully_reversed(backend)
        if unowned_manual_hold:
            _assert_unowned_manual_hold_untouched(backend)

    def test_known_bad_untouched_control_link_checker_trips(self) -> None:
        backend = FakeDeployHoldBackend()
        backend.control_links[WATCHDOG_TIMER] = "/dev/null"
        del backend.control_links[WATCHDOG_TIMER]  # simulates: touched anyway
        with self.assertRaisesRegex(AssertionError, "unowned control link"):
            _assert_unowned_control_links_untouched(
                backend, frozenset({WATCHDOG_TIMER}),
            )

    def test_known_bad_abort_ignoring_ownership_releases_an_unowned_hold(
        self,
    ) -> None:
        backend = FakeDeployHoldBackend(manual_hold=True)
        backend.create_receipt()

        _abort_hold_ignores_manual_hold_ownership(backend)

        with self.assertRaisesRegex(AssertionError, "did not own"):
            _assert_unowned_manual_hold_untouched(backend)


if __name__ == "__main__":
    unittest.main()
