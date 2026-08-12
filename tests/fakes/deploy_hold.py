"""State-respecting fake for the deploy hold's systemd/filesystem backend."""

from __future__ import annotations

from collections.abc import Iterable

from scripts.cratedigger_deploy_hold import (
    CONTROL_DIR,
    GATE_GUARDED_UNITS,
    IMPORTER_SERVICE,
    MAIN_SERVICE,
    PHASE_HELD,
    PREVIEW_SERVICE,
    SERVICE_UNITS,
    START_INHIBITORS,
    TIMER_UNITS,
    WEB_SERVICE,
    YOUTUBE_SERVICE,
    DeployHoldError,
    JobState,
    LifecyclePreflight,
    UnitState,
)

# Type=simple, wantedBy=multi-user.target, Restart=on-failure daemons with no
# timer: the real world every acquire actually meets has these already
# running, not idle. Defaulting them to "inactive" (as this fake once did)
# is more permissive than production in exactly the shape test-fidelity.md
# Rule B forbids -- it hid #1078 MUST FIX 1 (acquire hanging the full
# service-drain timeout waiting for YouTube ingest, which nothing before
# the gate hold ever asks to stop) behind 429 green targets.
_ALWAYS_ON_DAEMONS = (WEB_SERVICE, IMPORTER_SERVICE, PREVIEW_SERVICE, YOUTUBE_SERVICE)


class FakeDeployHoldBackend:
    """Model exact units, jobs, control links, and the runtime receipt."""

    def __init__(
        self,
        *,
        manual_hold: bool = False,
        metadata_holds: set[str] | None = None,
        control_links: dict[str, str] | None = None,
        jobs: dict[str, JobState] | None = None,
        running_samples: dict[str, int] | None = None,
        failed_services: set[str] | None = None,
        lifecycle_preflight: LifecyclePreflight | None = None,
        controlled_start_contract_current: bool = True,
        inhibitor_files: set[str] | None = None,
        interrupt_receipt_publication: bool = False,
        interrupt_receipt_retirement: bool = False,
        queue_drain_after_calls: int | None = None,
        persistent_manual_marker: bool = False,
        persistent_inhibitor_markers: set[str] | None = None,
    ) -> None:
        self.manual_hold = manual_hold
        self.other_metadata_holds = set(metadata_holds or set())
        self.control_links = dict(control_links or {})
        self.jobs = dict(jobs or {})
        self.running_samples = dict(running_samples or {})
        self.failed_services = set(failed_services or set())
        self.preflight = lifecycle_preflight or LifecyclePreflight(0, 0, 0, 0)
        self.controlled_start_contract_current = (
            controlled_start_contract_current
        )
        self.inhibitor_files = set(inhibitor_files or set())
        self.interrupt_receipt_publication = interrupt_receipt_publication
        self.interrupt_receipt_retirement = interrupt_receipt_retirement
        # Persistent siblings of the manual hold / producer inhibitors
        # (#1096): unlike every other field above, these survive reboot()
        # below -- they model /var/lib/cratedigger-metadata-gate, not /run.
        self.persistent_manual_marker = persistent_manual_marker
        self.persistent_inhibitor_markers = set(persistent_inhibitor_markers or set())
        # Models the causal claim behind #1078's reorder: the automation
        # queue only drains while the importer or preview worker is still
        # running. `active_automation_jobs`/`dirty_downloading_rows` latch to
        # 0 once `preflight_calls` exceeds this threshold, but ONLY on a call
        # where one of those workers is observed active -- so a caller that
        # stops them first (today's pre-#1078 order) never observes the
        # drain, no matter how many times it polls.
        self.queue_drain_after_calls = queue_drain_after_calls
        self._preflight_calls = 0
        self._queue_drained = False
        self.unit_states: dict[str, UnitState] = {
            **{
                timer: UnitState(
                    load_state="loaded",
                    active_state="active",
                    sub_state="waiting",
                )
                for timer in TIMER_UNITS
            },
            **{
                service: UnitState(
                    load_state="loaded",
                    active_state=(
                        "failed"
                        if service in self.failed_services
                        else "activating"
                        if self.jobs.get(service, JobState.none()).state == "running"
                        else "active"
                        if service in _ALWAYS_ON_DAEMONS
                        else "inactive"
                    ),
                    sub_state=(
                        "failed"
                        if service in self.failed_services
                        else "start"
                        if self.jobs.get(service, JobState.none()).state == "running"
                        else "running"
                        if service in _ALWAYS_ON_DAEMONS
                        else "dead"
                    ),
                )
                for service in SERVICE_UNITS
            },
        }
        self.receipt = False
        self.staging_receipt = False
        self.retired_receipt = False
        self.phase: str | None = None
        self.owned_links: set[str] = set()
        self.owned_inhibitors: set[str] = set()
        self.owned_manual_hold = False
        self.ordinary_invocation: str | None = None
        self.events: list[tuple[str, ...]] = []
        self.cancelled_jobs: list[str] = []
        self.started_units: list[str] = []
        self.sleep_calls = 0
        self._monotonic_seconds = 0.0

    def verify_controlled_start_contract(self) -> None:
        if not self.controlled_start_contract_current:
            raise DeployHoldError("controlled-start prerequisite changed")

    def lifecycle_preflight(self) -> LifecyclePreflight:
        self.events.append(("lifecycle-preflight",))
        self._preflight_calls += 1
        if not self._queue_drained:
            importer_or_preview_running = any(
                self.unit_states[service].active_state == "active"
                for service in (IMPORTER_SERVICE, PREVIEW_SERVICE)
            )
            if (
                self.queue_drain_after_calls is not None
                and self._preflight_calls > self.queue_drain_after_calls
                and importer_or_preview_running
            ):
                self._queue_drained = True
        if self._queue_drained:
            return LifecyclePreflight(
                active_automation_jobs=0,
                recovery_required_jobs=self.preflight.recovery_required_jobs,
                dirty_downloading_rows=0,
                malformed_enqueued_at_rows=self.preflight.malformed_enqueued_at_rows,
            )
        return self.preflight

    def ensure_control_dir(self) -> None:
        pass

    def receipt_exists(self) -> bool:
        return self.receipt

    def retired_receipt_exists(self) -> bool:
        return self.retired_receipt

    def create_receipt(self) -> None:
        if self.receipt:
            raise FileExistsError("receipt exists")
        if self.staging_receipt:
            self.events.append(("receipt-staging-clean",))
            self.staging_receipt = False
        if self.interrupt_receipt_publication:
            self.interrupt_receipt_publication = False
            self.staging_receipt = True
            self.events.append(("receipt-staging",))
            raise InterruptedError("injected receipt-publication interruption")
        self.receipt = True
        self.phase = "acquiring"
        self.events.append(("receipt-create",))

    def remove_receipt(self) -> None:
        self.events.append(("receipt-retire",))
        self.receipt = False
        self.retired_receipt = True
        self.phase = None
        self.owned_links.clear()
        self.owned_inhibitors.clear()
        self.owned_manual_hold = False
        self.ordinary_invocation = None
        if self.interrupt_receipt_retirement:
            self.interrupt_receipt_retirement = False
            raise InterruptedError("injected receipt-retirement interruption")
        self.finish_retired_receipt()

    def finish_retired_receipt(self) -> None:
        if not self.retired_receipt:
            raise FileNotFoundError("retired receipt missing")
        self.events.append(("receipt-remove",))
        self.retired_receipt = False

    def read_phase(self) -> str:
        if not self.receipt or self.phase is None:
            raise FileNotFoundError("receipt missing")
        return self.phase

    def write_phase(self, phase: str) -> None:
        if not self.receipt:
            raise FileNotFoundError("receipt missing")
        self.phase = phase
        self.events.append(("phase", phase))

    def mark_manual_hold_owned(self) -> None:
        # Persistent marker first, mirroring RealSystemdBackend: it must
        # exist before the manual hold object itself does (#1096).
        self.write_persistent_manual_marker()
        self.owned_manual_hold = True
        self.events.append(("own-manual",))

    def unmark_manual_hold_owned(self) -> None:
        self.remove_persistent_manual_marker()
        self.owned_manual_hold = False
        self.events.append(("disown-manual",))

    def manual_hold_is_owned(self) -> bool:
        return self.owned_manual_hold

    def persistent_manual_marker_exists(self) -> bool:
        return self.persistent_manual_marker

    def write_persistent_manual_marker(self) -> None:
        self.persistent_manual_marker = True
        self.events.append(("persist-own-manual",))

    def remove_persistent_manual_marker(self) -> None:
        self.persistent_manual_marker = False
        self.events.append(("persist-disown-manual",))

    def mark_link_owned(self, timer: str) -> None:
        self.owned_links.add(timer)
        self.events.append(("own-link", timer))

    def unmark_link_owned(self, timer: str) -> None:
        self.owned_links.remove(timer)
        self.events.append(("disown-link", timer))

    def link_is_owned(self, timer: str) -> bool:
        return timer in self.owned_links

    def owned_link_units(self) -> tuple[str, ...]:
        return tuple(sorted(self.owned_links))

    def mark_inhibitor_owned(self, service: str) -> None:
        if service not in START_INHIBITORS:
            raise AssertionError(f"unexpected inhibitor service: {service}")
        self.write_persistent_inhibitor_marker(service)
        self.owned_inhibitors.add(service)
        self.events.append(("own-inhibitor", service))

    def unmark_inhibitor_owned(self, service: str) -> None:
        self.remove_persistent_inhibitor_marker(service)
        self.owned_inhibitors.remove(service)
        self.events.append(("disown-inhibitor", service))

    def inhibitor_is_owned(self, service: str) -> bool:
        return service in self.owned_inhibitors

    def owned_inhibitor_units(self) -> tuple[str, ...]:
        return tuple(sorted(self.owned_inhibitors))

    def persistent_inhibitor_marker_exists(self, service: str) -> bool:
        return service in self.persistent_inhibitor_markers

    def write_persistent_inhibitor_marker(self, service: str) -> None:
        if service not in START_INHIBITORS:
            raise AssertionError(f"unexpected inhibitor service: {service}")
        self.persistent_inhibitor_markers.add(service)
        self.events.append(("persist-own-inhibitor", service))

    def remove_persistent_inhibitor_marker(self, service: str) -> None:
        self.persistent_inhibitor_markers.discard(service)
        self.events.append(("persist-disown-inhibitor", service))

    def inhibitor_exists(self, service: str) -> bool:
        return service in self.inhibitor_files

    def create_start_inhibitor(self, service: str) -> None:
        if service in self.inhibitor_files:
            raise FileExistsError(service)
        self.inhibitor_files.add(service)
        self.events.append(("inhibitor-create", service))

    def remove_start_inhibitor(self, service: str) -> None:
        self.inhibitor_files.remove(service)
        self.events.append(("inhibitor-remove", service))

    def write_ordinary_invocation(self, invocation_id: str) -> None:
        self.ordinary_invocation = invocation_id
        self.events.append(("ordinary-invocation", invocation_id))

    def read_ordinary_invocation(self) -> str:
        if self.ordinary_invocation is None:
            raise FileNotFoundError("ordinary invocation missing")
        return self.ordinary_invocation

    def clear_ordinary_invocation(self) -> None:
        self.ordinary_invocation = None
        self.events.append(("ordinary-invocation-clear",))

    def manual_hold_active(self) -> bool:
        return self.manual_hold

    def metadata_gate(self, command: str) -> int:
        self.events.append(("metadata-gate", command))
        if command == "hold manual":
            self.manual_hold = True
            # Iterate GATE_GUARDED_UNITS directly, not a module-local alias
            # (#1100 item 1 -- this fake once silently drifted from the
            # gate's real guarded set). Timer/main.service fidelity here is
            # proven by TestFakeGateHoldModelsTheRealGuardedSet in
            # tests/test_deploy_hold.py, not by acquire/recover/prepare
            # tests; stopping a wider set is the test-fidelity.md Rule B
            # smell this once was.
            for unit in GATE_GUARDED_UNITS:
                state = self.unit_states[unit]
                if state.active_state == "active":
                    self.unit_states[unit] = UnitState(
                        load_state=state.load_state,
                        active_state="inactive",
                        sub_state="dead",
                    )
        elif command == "release manual":
            self.manual_hold = False
        elif command != "resume-if-clear":
            raise AssertionError(f"unexpected metadata gate command: {command}")
        return 1 if self.manual_hold or self.other_metadata_holds else 0

    def metadata_hold_reasons(self) -> tuple[str, ...]:
        reasons = set(self.other_metadata_holds)
        if self.manual_hold:
            reasons.add("manual")
        return tuple(sorted(reasons))

    def control_link_target(self, timer: str) -> str | None:
        return self.control_links.get(timer)

    def create_control_mask(self, timer: str) -> None:
        if timer in self.control_links:
            raise FileExistsError(timer)
        self.control_links[timer] = "/dev/null"
        self.events.append(("link-create", f"{CONTROL_DIR}/{timer}"))

    def remove_control_mask(self, timer: str) -> None:
        del self.control_links[timer]
        self.events.append(("link-remove", f"{CONTROL_DIR}/{timer}"))

    def daemon_reload(self) -> None:
        self.events.append(("daemon-reload",))
        for timer in TIMER_UNITS:
            state = self.unit_states[timer]
            self.unit_states[timer] = UnitState(
                load_state=(
                    "masked"
                    if self.control_links.get(timer) == "/dev/null"
                    else "loaded"
                ),
                active_state=state.active_state,
                sub_state=state.sub_state,
            )

    def stop_units(self, units: Iterable[str]) -> None:
        exact = tuple(units)
        self.events.append(("stop", *exact))
        for unit in exact:
            state = self.unit_states[unit]
            self.unit_states[unit] = UnitState(
                load_state=state.load_state,
                active_state="inactive",
                sub_state="dead",
            )

    def start_unit(self, unit: str) -> None:
        self.events.append(("start", unit))
        self.started_units.append(unit)
        state = self.unit_states[unit]
        if (state.active_state, state.sub_state) == ("active", "running"):
            # Real systemd: a start job against an already-active unit is a
            # no-op -- it does not re-evaluate ExecCondition or re-run
            # ExecStart. Modeled explicitly rather than falling through to
            # the branches below, which would be harmless here but is not
            # what production actually does.
            return
        # Real systemd: ConditionPathExists/ExecCondition is evaluated on
        # every start attempt. A gate-guarded SERVICE's condition fails while
        # any metadata-gate hold is active (module-vm.nix's
        # metadataGateStartCheck checks the WHOLE holds directory is empty,
        # not just that no foreign reason exists); main/YouTube additionally
        # each fail while their own producer inhibitor file exists. Either
        # failure is a condition SKIP, not a job failure: systemctl still
        # exits 0, but the unit's state is untouched (#1096 correction
        # round -- this fidelity gap is what let the M1/M2 ordering bugs in
        # _adopt_persistent_markers_or_refuse and abort_hold ship green).
        # ``ExecCondition=`` is a ``[Service]`` directive -- module-vm.nix's
        # own ``metadataGateServiceNames`` wires it onto every gate-guarded
        # unit EXCEPT ``cratedigger.timer`` (GATE_GUARDED_UNITS' only timer
        # member), because a ``.timer`` unit has no ``[Service]`` section to
        # carry it at all; a real hold cannot condition-skip a timer's own
        # start. Excluding TIMER_UNITS here is what makes that true in the
        # fake too (#1096 review round 2, F1) -- without it, an acquiring
        # receipt owning only links plus a foreign hold would fail abort's
        # timer restart in the fake while production restarts it cleanly.
        blocked = (unit in START_INHIBITORS and unit in self.inhibitor_files) or (
            unit in GATE_GUARDED_UNITS
            and unit not in TIMER_UNITS
            and (self.manual_hold or self.other_metadata_holds)
        )
        if blocked:
            return
        if unit == MAIN_SERVICE:
            # The PR1 verifier lives outside this state machine. Deterministic
            # hold tests model that verified completion before open-main-timer.
            self.unit_states[unit] = UnitState(
                load_state=state.load_state,
                active_state="inactive",
                sub_state="dead",
            )
        else:
            self.unit_states[unit] = UnitState(
                load_state=state.load_state,
                active_state="active",
                sub_state=("waiting" if unit.endswith(".timer") else "running"),
            )

    def unit_state(self, unit: str) -> UnitState:
        return self.unit_states[unit]

    def job_state(self, unit: str) -> JobState:
        return self.jobs.get(unit, JobState.none())

    def cancel_job(self, job_id: str) -> None:
        matching = [
            unit for unit, job in self.jobs.items() if job.job_id == job_id
        ]
        if len(matching) != 1:
            raise AssertionError(f"unknown job: {job_id}")
        unit = matching[0]
        self.cancelled_jobs.append(job_id)
        self.events.append(("cancel-job", job_id, unit))
        del self.jobs[unit]

    def reset_failed(self, unit: str) -> None:
        state = self.unit_states[unit]
        if (state.active_state, state.sub_state) != ("failed", "failed"):
            raise AssertionError(f"reset of non-failed service: {unit}")
        self.unit_states[unit] = UnitState(
            load_state=state.load_state,
            active_state="inactive",
            sub_state="dead",
        )
        self.failed_services.discard(unit)
        self.events.append(("reset-failed", unit))

    def monotonic(self) -> float:
        return self._monotonic_seconds

    def sleep(self, seconds: float) -> None:
        # Advances by the real requested duration (not a fixed +1 per call)
        # so every production timeout/poll-interval constant governs this
        # fake exactly as it governs production, with no need to patch a
        # constant down for test speed: the fake's sleep is instant, so even
        # the full production timeout is a fast, bounded number of Python
        # loop iterations here.
        self._monotonic_seconds += seconds
        self.sleep_calls += 1
        self.events.append(("sleep",))
        for unit in tuple(SERVICE_UNITS):
            remaining = self.running_samples.get(unit)
            if remaining is None:
                continue
            if remaining > 0:
                self.running_samples[unit] = remaining - 1
                continue
            self.jobs.pop(unit, None)
            state = self.unit_states[unit]
            self.unit_states[unit] = UnitState(
                load_state=state.load_state,
                active_state="inactive",
                sub_state="dead",
            )
            del self.running_samples[unit]

    def reboot(self) -> None:
        """Model a real, graceful host reboot: wipe tmpfs, retain
        persistent state durably.

        Clears the receipt, phase, every tmpfs ownership marker (manual
        hold, control links, inhibitors), and the ordinary-invocation
        marker -- all ``/run``, all gone, exactly like a graceful reboot
        clears the module-vm.nix test VM's tmpfs root
        (``machine.shutdown()`` there, deliberately not ``machine.crash()``:
        an abrupt power failure can leave a very recent ``/var/lib``
        ``unlink()`` non-durable under ordinary ext4 write-back caching --
        a real but entirely separate crash-consistency concern this fake
        does not model). RETAINS the manual hold / inhibitor objects
        themselves, their sibling persistent markers, and every other
        metadata-gate hold file -- all ``/var/lib``, all durable (#1096).

        Unit states reset to post-boot reality rather than merely
        preserving whatever they were before the reboot: a fresh boot
        starts every unit from its own systemd wantedBy/Condition wiring,
        not from a snapshot of what was running a moment before the crash.
        A gate-guarded unit (``GATE_GUARDED_UNITS``) stays condition-blocked
        if the persistent manual hold or its own persistent inhibitor is
        still present; every other always-on daemon resumes; timer-driven
        oneshots and the timers themselves come up idle/active exactly as
        they do on an ordinary first boot.
        """
        self.receipt = False
        self.staging_receipt = False
        self.phase = None
        self.owned_links.clear()
        self.owned_inhibitors.clear()
        self.owned_manual_hold = False
        self.ordinary_invocation = None
        self.control_links.clear()
        self.jobs.clear()
        self.running_samples.clear()
        self.cancelled_jobs.clear()
        self.started_units.clear()
        self.failed_services.clear()
        self.events.append(("reboot",))

        def blocked(service: str) -> bool:
            if service not in GATE_GUARDED_UNITS:
                return False
            if self.manual_hold:
                return True
            return service in START_INHIBITORS and service in self.inhibitor_files

        self.unit_states = {
            **{
                timer: UnitState(
                    load_state="loaded",
                    active_state="active",
                    sub_state="waiting",
                )
                for timer in TIMER_UNITS
            },
            **{
                service: UnitState(
                    load_state="loaded",
                    active_state=(
                        "inactive"
                        if blocked(service)
                        else "active"
                        if service in _ALWAYS_ON_DAEMONS
                        else "inactive"
                    ),
                    sub_state=(
                        "dead"
                        if blocked(service)
                        else "running"
                        if service in _ALWAYS_ON_DAEMONS
                        else "dead"
                    ),
                )
                for service in SERVICE_UNITS
            },
        }

    def assert_default_held(self) -> None:
        assert self.phase == PHASE_HELD
        assert self.manual_hold
        assert self.owned_manual_hold
        assert self.owned_links == set(TIMER_UNITS)
