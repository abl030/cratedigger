"""Generated patrols for fail-closed execution liveness."""

import unittest

from hypothesis import example, given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401
from lib.import_execution import (
    CgroupObservation,
    CgroupState,
    ExecutionLeaseSnapshot,
    ExecutionLivenessDecision,
    ExecutionLivenessEvidence,
    InvocationObservation,
    InvocationState,
    ProcessIdentity,
    ProcessObservation,
    ProcessState,
    decide_execution_liveness,
)


def assert_liveness_decision_safe(
    evidence: ExecutionLivenessEvidence,
    decision: ExecutionLivenessDecision,
) -> None:
    """Only complete positive presence/death proof may escape unknown."""
    lease = evidence.lease
    if lease is None:
        if decision.status != "dead" or decision.reason != "never_claimed":
            raise AssertionError("lease-free execution was not never-claimed")
        return
    if (
        evidence.probe_error is not None
        or evidence.boot_error is not None
        or evidence.current_host_boot_id is None
    ):
        if decision.status != "unknown":
            raise AssertionError("probe/read failure escaped unknown")
        return
    if evidence.current_host_boot_id != lease.host_boot_id:
        if decision.status != "dead" or decision.reason != "boot_changed":
            raise AssertionError("changed boot did not prove execution dead")
        return
    processes = tuple(
        process
        for process in (evidence.worker, evidence.beets)
        if process is not None
    )
    if (
        evidence.worker is None
        or (lease.beets is not None and evidence.beets is None)
        or evidence.invocation is None
        or evidence.cgroup is None
    ):
        if decision.status != "unknown":
            raise AssertionError("missing observation escaped unknown")
        return
    if decision.status == "live":
        if evidence.current_host_boot_id != lease.host_boot_id:
            raise AssertionError("live decision crossed a host boot")
        if evidence.invocation is None or evidence.invocation.state != "exact":
            raise AssertionError("live decision lacks exact invocation")
        if evidence.cgroup is None or evidence.cgroup.state != "exact":
            raise AssertionError("live decision lacks exact cgroup")
        if any(
            process is not None and process.state == "unknown"
            for process in processes
        ):
            raise AssertionError("live decision ignored unknown process")
    if decision.status == "dead" and decision.reason == "execution_ended":
        if evidence.invocation is None or evidence.invocation.state != "ended":
            raise AssertionError("dead decision lacks ended invocation")
        if evidence.cgroup is None or evidence.cgroup.state != "absent":
            raise AssertionError("dead decision lacks absent cgroup")
        if any(
            process is not None
            and process.state not in {"absent", "reused"}
            for process in (evidence.worker, evidence.beets)
        ):
            raise AssertionError("dead decision ignored possible process")
    complete_live = (
        evidence.boot_error is None
        and evidence.worker is not None
        and (lease.beets is None or evidence.beets is not None)
        and not any(
            process is not None and process.state == "unknown"
            for process in processes
        )
        and evidence.invocation is not None
        and evidence.invocation.state == "exact"
        and evidence.cgroup is not None
        and evidence.cgroup.state == "exact"
    )
    if complete_live and decision.status != "live":
        raise AssertionError("complete live proof was ignored")
    complete_death = (
        evidence.boot_error is None
        and evidence.worker is not None
        and (lease.beets is None or evidence.beets is not None)
        and all(
            process.state in {"absent", "reused"}
            for process in processes
        )
        and evidence.invocation is not None
        and evidence.invocation.state == "ended"
        and evidence.cgroup is not None
        and evidence.cgroup.state == "absent"
    )
    if complete_death and decision.status != "dead":
        raise AssertionError("complete death proof was ignored")


_PROCESS_STATES = st.sampled_from(("exact", "absent", "reused", "unknown"))
_INVOCATION_STATES = st.sampled_from(
    ("exact", "ended", "conflict", "unknown")
)
_CGROUP_STATES = st.sampled_from(("exact", "absent", "conflict", "unknown"))


class TestExecutionLivenessGenerated(unittest.TestCase):
    @given(
        same_boot=st.booleans(),
        worker_state=_PROCESS_STATES,
        child_state=_PROCESS_STATES,
        invocation_state=_INVOCATION_STATES,
        cgroup_state=_CGROUP_STATES,
        missing_worker=st.booleans(),
        missing_child=st.booleans(),
        missing_invocation=st.booleans(),
        missing_cgroup=st.booleans(),
        boot_error=st.booleans(),
        probe_error=st.booleans(),
    )
    @example(
        same_boot=True,
        worker_state="reused",
        child_state="absent",
        invocation_state="ended",
        cgroup_state="absent",
        missing_worker=False,
        missing_child=False,
        missing_invocation=False,
        missing_cgroup=False,
        boot_error=False,
        probe_error=False,
    )
    def test_only_complete_proof_escapes_unknown(
        self,
        *,
        same_boot: bool,
        worker_state: ProcessState,
        child_state: ProcessState,
        invocation_state: InvocationState,
        cgroup_state: CgroupState,
        missing_worker: bool,
        missing_child: bool,
        missing_invocation: bool,
        missing_cgroup: bool,
        boot_error: bool,
        probe_error: bool,
    ) -> None:
        beets_identity = ProcessIdentity(202, 2002)
        lease = ExecutionLeaseSnapshot(
            host_boot_id="boot-a",
            invocation_id="invocation-a",
            systemd_unit="cratedigger-importer.service",
            worker=ProcessIdentity(101, 1001),
            beets=beets_identity,
        )
        evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=(
                None
                if boot_error or probe_error
                else "boot-a" if same_boot else "boot-b"
            ),
            boot_error="boot_read_failed" if boot_error else None,
            worker=(
                None
                if missing_worker or boot_error or probe_error or not same_boot
                else ProcessObservation(
                    lease.worker,
                    worker_state,
                    (
                        lease.worker.start_ticks
                        if worker_state == "exact"
                        else lease.worker.start_ticks + 1
                        if worker_state == "reused"
                        else None
                    ),
                    (
                        "/system.slice/service"
                        if worker_state == "exact" else None
                    ),
                    worker_state,
                )
            ),
            beets=ProcessObservation(
                beets_identity,
                child_state,
                (
                    beets_identity.start_ticks
                    if child_state == "exact"
                    else beets_identity.start_ticks + 1
                    if child_state == "reused"
                    else None
                ),
                "/system.slice/service" if child_state == "exact" else None,
                child_state,
            ) if (
                not missing_child
                and not boot_error
                and not probe_error
                and same_boot
            ) else None,
            invocation=(
                None
                if (
                    missing_invocation
                    or boot_error
                    or probe_error
                    or not same_boot
                )
                else InvocationObservation(
                    invocation_state,
                    lease.invocation_id,
                    (
                        lease.invocation_id
                        if invocation_state == "exact" else None
                    ),
                    (
                        "/system.slice/service"
                        if invocation_state == "exact" else None
                    ),
                    invocation_state,
                )
            ),
            cgroup=(
                None
                if missing_cgroup or boot_error or probe_error or not same_boot
                else CgroupObservation(
                    cgroup_state,
                    (
                        "/system.slice/service"
                        if cgroup_state == "exact" else None
                    ),
                    (101, 202) if cgroup_state == "exact" else (),
                    cgroup_state,
                )
            ),
            probe_error="probe_failed" if probe_error else None,
        )

        decision = decide_execution_liveness(evidence)

        assert_liveness_decision_safe(evidence, decision)

    @given(
        wrong_pid=st.integers(min_value=1, max_value=100_000).filter(
            lambda pid: pid != 101
        ),
        wrong_ticks=st.integers(min_value=0, max_value=100_000).filter(
            lambda ticks: ticks != 1001
        ),
    )
    def test_inconsistent_process_identity_cannot_enter_evidence(
        self,
        *,
        wrong_pid: int,
        wrong_ticks: int,
    ) -> None:
        lease = ExecutionLeaseSnapshot(
            "boot-a",
            "invocation-a",
            "cratedigger-importer.service",
            ProcessIdentity(101, 1001),
        )
        with self.assertRaises(ValueError):
            ProcessObservation(
                lease.worker,
                "exact",
                wrong_ticks,
                "/system.slice/service",
                "wrong_start_ticks",
            )
        with self.assertRaises(ValueError):
            ExecutionLivenessEvidence(
                lease=lease,
                current_host_boot_id="boot-a",
                boot_error=None,
                worker=ProcessObservation(
                    ProcessIdentity(wrong_pid, wrong_ticks),
                    "exact",
                    wrong_ticks,
                    "/system.slice/service",
                    "wrong_identity",
                ),
                beets=None,
                invocation=None,
                cgroup=None,
            )


class TestLivenessCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_live_pid_reuse_mutant(self) -> None:
        lease = ExecutionLeaseSnapshot(
            "boot-a",
            "invocation-a",
            "cratedigger-importer.service",
            ProcessIdentity(101, 1001),
        )
        evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-a",
            boot_error=None,
            worker=ProcessObservation(
                lease.worker, "reused", 9999, None, "pid_reused"
            ),
            beets=None,
            invocation=InvocationObservation(
                "ended", lease.invocation_id, None, None, "ended",
            ),
            cgroup=CgroupObservation(
                "absent", None, (), "absent"
            ),
        )
        mutant = ExecutionLivenessDecision("live", "mutant", evidence)

        with self.assertRaisesRegex(AssertionError, "exact invocation"):
            assert_liveness_decision_safe(evidence, mutant)

    def test_checker_rejects_dead_with_live_process_mutant(self) -> None:
        lease = ExecutionLeaseSnapshot(
            "boot-a",
            "invocation-a",
            "cratedigger-importer.service",
            ProcessIdentity(101, 1001),
        )
        evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-a",
            boot_error=None,
            worker=ProcessObservation(
                lease.worker, "exact", 1001, "/system.slice/service", "exact"
            ),
            beets=None,
            invocation=InvocationObservation(
                "ended", lease.invocation_id, None, None, "ended"
            ),
            cgroup=CgroupObservation("absent", None, (), "absent"),
        )
        mutant = ExecutionLivenessDecision(
            "dead", "execution_ended", evidence
        )

        with self.assertRaisesRegex(AssertionError, "possible process"):
            assert_liveness_decision_safe(evidence, mutant)

    def test_checker_rejects_pid_reuse_as_permanent_unknown_mutant(
        self,
    ) -> None:
        lease = ExecutionLeaseSnapshot(
            "boot-a",
            "invocation-a",
            "cratedigger-importer.service",
            ProcessIdentity(101, 1001),
        )
        evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-a",
            boot_error=None,
            worker=ProcessObservation(
                lease.worker, "reused", 9999, None, "pid_reused"
            ),
            beets=None,
            invocation=InvocationObservation(
                "ended", lease.invocation_id, None, None, "ended"
            ),
            cgroup=CgroupObservation("absent", None, (), "absent"),
        )
        mutant = ExecutionLivenessDecision("unknown", "pid_reused", evidence)

        with self.assertRaisesRegex(AssertionError, "death proof"):
            assert_liveness_decision_safe(evidence, mutant)
