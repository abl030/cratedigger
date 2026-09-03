"""Fail-stop execution identity, liveness, cancellation, and process groups.

Execution leases are evidence about one worker invocation. They never grant
request or import-job authority; owner-aware database predicates do that.
"""

from __future__ import annotations

import os
import pathlib
import signal
import subprocess
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, Self

LivenessStatus = Literal["live", "dead", "unknown"]
ProcessState = Literal["exact", "absent", "reused", "unknown"]
InvocationState = Literal["exact", "ended", "conflict", "unknown"]
CgroupState = Literal["exact", "absent", "conflict", "unknown"]


@dataclass(frozen=True)
class ProcessIdentity:
    """Linux PID identity protected against PID reuse by start ticks."""

    pid: int
    start_ticks: int

    def __post_init__(self) -> None:
        if self.pid <= 0:
            raise ValueError("process PID must be positive")
        if self.start_ticks < 0:
            raise ValueError("process start ticks must be non-negative")


@dataclass(frozen=True)
class ExecutionLeaseSnapshot:
    """Complete persisted identity for one importer execution."""

    host_boot_id: str
    invocation_id: str
    systemd_unit: str
    worker: ProcessIdentity
    beets: ProcessIdentity | None = None

    def __post_init__(self) -> None:
        if not self.host_boot_id.strip():
            raise ValueError("host boot ID must be nonblank")
        if not self.invocation_id.strip():
            raise ValueError("INVOCATION_ID must be nonblank")
        if not self.systemd_unit.strip():
            raise ValueError("explicit systemd unit must be nonblank")


@dataclass(frozen=True)
class ProcessObservation:
    identity: ProcessIdentity
    state: ProcessState
    observed_start_ticks: int | None
    cgroup_path: str | None
    reason: str

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("process observation reason must be nonblank")
        if self.observed_start_ticks is not None \
                and self.observed_start_ticks < 0:
            raise ValueError("observed process start ticks must be non-negative")
        if self.cgroup_path is not None and not self.cgroup_path.startswith("/"):
            raise ValueError("process cgroup path must be absolute")
        if self.state == "exact":
            if self.observed_start_ticks != self.identity.start_ticks:
                raise ValueError("exact process observation changed start ticks")
            if self.cgroup_path is None:
                raise ValueError("exact process observation needs its cgroup")
        elif self.state == "absent":
            if self.observed_start_ticks is not None or self.cgroup_path is not None:
                raise ValueError("absent process cannot have observed identity")
        elif self.state == "reused":
            if (
                self.observed_start_ticks is None
                or self.observed_start_ticks == self.identity.start_ticks
            ):
                raise ValueError("reused PID needs different start ticks")


@dataclass(frozen=True)
class InvocationObservation:
    state: InvocationState
    stored_invocation_id: str
    observed_invocation_id: str | None
    control_group: str | None
    reason: str
    active_state: str | None = None
    sub_state: str | None = None

    def __post_init__(self) -> None:
        if not self.stored_invocation_id.strip():
            raise ValueError("stored invocation ID must be nonblank")
        if self.observed_invocation_id is not None \
                and not self.observed_invocation_id.strip():
            raise ValueError("observed invocation ID must be nonblank")
        if self.control_group is not None \
                and not self.control_group.startswith("/"):
            raise ValueError("unit control group must be absolute")
        if not self.reason.strip():
            raise ValueError("invocation observation reason must be nonblank")
        if self.state == "exact" \
                and self.observed_invocation_id != self.stored_invocation_id:
            raise ValueError("exact invocation observation changed identity")
        if self.state == "exact" and self.active_state in {
            "inactive",
            "failed",
        }:
            raise ValueError("inactive systemd invocation cannot be exact")
        if (
            self.state == "ended"
            and self.observed_invocation_id == self.stored_invocation_id
            and self.active_state not in {"inactive", "failed"}
        ):
            raise ValueError(
                "retained invocation is ended only when inactive or failed"
            )
        if (self.active_state is None) != (self.sub_state is None):
            raise ValueError(
                "systemd active and sub states must be observed together"
            )


@dataclass(frozen=True)
class CgroupObservation:
    state: CgroupState
    path: str | None
    member_pids: tuple[int, ...]
    reason: str

    def __post_init__(self) -> None:
        if self.path is not None and not self.path.startswith("/"):
            raise ValueError("observed cgroup path must be absolute")
        if any(pid <= 0 for pid in self.member_pids):
            raise ValueError("cgroup member PIDs must be positive")
        if len(set(self.member_pids)) != len(self.member_pids):
            raise ValueError("cgroup member PIDs must be unique")
        if not self.reason.strip():
            raise ValueError("cgroup observation reason must be nonblank")
        if self.state == "exact" and (
            self.path is None or not self.member_pids
        ):
            raise ValueError("exact cgroup needs a path and live members")
        if self.state == "absent" and self.member_pids:
            raise ValueError("absent cgroup cannot contain live members")


@dataclass(frozen=True)
class ExecutionLivenessEvidence:
    """One reproducible liveness transcript."""

    lease: ExecutionLeaseSnapshot | None
    current_host_boot_id: str | None
    boot_error: str | None
    worker: ProcessObservation | None
    beets: ProcessObservation | None
    invocation: InvocationObservation | None
    cgroup: CgroupObservation | None
    probe_error: str | None = None

    def __post_init__(self) -> None:
        observations = (
            self.worker,
            self.beets,
            self.invocation,
            self.cgroup,
        )
        if self.probe_error is not None and any(
            observation is not None for observation in observations
        ):
            raise ValueError("failed probe cannot carry partial observations")
        if self.boot_error is not None and (
            self.current_host_boot_id is not None
            or any(observation is not None for observation in observations)
        ):
            raise ValueError("failed boot probe cannot carry observations")
        if self.lease is None:
            if any(
                observation is not None
                for observation in observations
            ):
                raise ValueError("lease-free evidence cannot have observations")
            return
        if (
            self.probe_error is None
            and self.boot_error is None
            and self.current_host_boot_id is None
        ):
            raise ValueError("execution evidence needs the current boot ID")
        if self.current_host_boot_id is not None \
                and not self.current_host_boot_id.strip():
            raise ValueError("current boot ID must be nonblank")
        if (
            self.current_host_boot_id is not None
            and self.current_host_boot_id != self.lease.host_boot_id
            and any(observation is not None for observation in observations)
        ):
            raise ValueError("changed-boot evidence cannot reuse observations")
        if self.worker is not None \
                and self.worker.identity != self.lease.worker:
            raise ValueError("worker observation has the wrong identity")
        if self.lease.beets is None:
            if self.beets is not None:
                raise ValueError("unexpected Beets process observation")
        elif self.beets is not None and self.beets.identity != self.lease.beets:
            raise ValueError("Beets observation has the wrong identity")
        if (
            self.invocation is not None
            and self.invocation.stored_invocation_id
            != self.lease.invocation_id
        ):
            raise ValueError("invocation observation has the wrong lease")


@dataclass(frozen=True)
class ExecutionLivenessDecision:
    status: LivenessStatus
    reason: str
    evidence: ExecutionLivenessEvidence


class ExecutionLivenessProbe(Protocol):
    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence: ...


def _read_nonblank(path: pathlib.Path, label: str) -> str:
    value = path.read_text().strip()
    if not value:
        raise OSError(f"{label} is blank")
    return value


def read_process_start_ticks(
    pid: int,
    *,
    proc_root: pathlib.Path = pathlib.Path("/proc"),
) -> int:
    """Read Linux ``/proc/<pid>/stat`` field 22 without splitting ``comm``."""
    stat = (proc_root / str(pid) / "stat").read_text()
    closing_paren = stat.rfind(")")
    if closing_paren < 0:
        raise OSError(f"malformed /proc/{pid}/stat")
    after_comm = stat[closing_paren + 1:].split()
    # ``after_comm[0]`` is field 3 (state); starttime is field 22.
    if len(after_comm) <= 19:
        raise OSError(f"truncated /proc/{pid}/stat")
    try:
        ticks = int(after_comm[19])
    except ValueError as exc:
        raise OSError(f"invalid /proc/{pid}/stat start ticks") from exc
    if ticks < 0:
        raise OSError(f"negative /proc/{pid}/stat start ticks")
    return ticks


def capture_execution_lease(
    *,
    systemd_unit: str,
    invocation_id: str | None = None,
    worker_pid: int | None = None,
    beets_pid: int | None = None,
    proc_root: pathlib.Path = pathlib.Path("/proc"),
    boot_id_path: pathlib.Path = pathlib.Path(
        "/proc/sys/kernel/random/boot_id"
    ),
    environ: Mapping[str, str] | None = None,
) -> ExecutionLeaseSnapshot:
    """Capture the current invocation and exact Linux process identities."""
    source_environ = os.environ if environ is None else environ
    observed_invocation = (
        source_environ.get("INVOCATION_ID")
        if invocation_id is None else invocation_id
    )
    if observed_invocation is None or not observed_invocation.strip():
        raise ValueError("INVOCATION_ID must be nonblank")
    if not systemd_unit.strip():
        raise ValueError("explicit systemd unit must be nonblank")
    observed_worker_pid = os.getpid() if worker_pid is None else worker_pid
    worker = ProcessIdentity(
        observed_worker_pid,
        read_process_start_ticks(observed_worker_pid, proc_root=proc_root),
    )
    beets = (
        ProcessIdentity(
            beets_pid,
            read_process_start_ticks(beets_pid, proc_root=proc_root),
        )
        if beets_pid is not None else None
    )
    return ExecutionLeaseSnapshot(
        host_boot_id=_read_nonblank(boot_id_path, "host boot ID"),
        invocation_id=observed_invocation,
        systemd_unit=systemd_unit,
        worker=worker,
        beets=beets,
    )


def decide_execution_liveness(
    evidence: ExecutionLivenessEvidence,
) -> ExecutionLivenessDecision:
    """Apply the one fail-closed execution-liveness decision table."""
    lease = evidence.lease
    if lease is None:
        return ExecutionLivenessDecision("dead", "never_claimed", evidence)
    if evidence.probe_error is not None:
        return ExecutionLivenessDecision("unknown", "probe_failed", evidence)
    if evidence.boot_error is not None or evidence.current_host_boot_id is None:
        return ExecutionLivenessDecision("unknown", "boot_unknown", evidence)
    if evidence.current_host_boot_id != lease.host_boot_id:
        return ExecutionLivenessDecision("dead", "boot_changed", evidence)

    processes = tuple(
        process
        for process in (evidence.worker, evidence.beets)
        if process is not None
    )
    if evidence.worker is None:
        return ExecutionLivenessDecision(
            "unknown", "worker_probe_missing", evidence
        )
    if lease.beets is not None and evidence.beets is None:
        return ExecutionLivenessDecision(
            "unknown", "beets_probe_missing", evidence
        )
    unknown_process = next(
        (
            process
            for process in processes
            if process.state == "unknown"
        ),
        None,
    )
    if unknown_process is not None:
        return ExecutionLivenessDecision(
            "unknown", unknown_process.reason, evidence
        )
    if evidence.invocation is None:
        return ExecutionLivenessDecision(
            "unknown", "invocation_probe_missing", evidence
        )
    if evidence.cgroup is None:
        return ExecutionLivenessDecision(
            "unknown", "cgroup_probe_missing", evidence
        )
    if evidence.invocation.state in {"conflict", "unknown"}:
        return ExecutionLivenessDecision(
            "unknown", evidence.invocation.reason, evidence
        )
    if evidence.cgroup.state in {"conflict", "unknown"}:
        return ExecutionLivenessDecision(
            "unknown", evidence.cgroup.reason, evidence
        )

    if (
        evidence.invocation.state == "exact"
        and evidence.cgroup.state == "exact"
    ):
        return ExecutionLivenessDecision(
            "live", "exact_execution_present", evidence
        )
    if (
        evidence.invocation.state == "ended"
        and evidence.cgroup.state == "absent"
        and all(
            process.state in {"absent", "reused"}
            for process in processes
        )
    ):
        return ExecutionLivenessDecision("dead", "execution_ended", evidence)
    return ExecutionLivenessDecision(
        "unknown", "incomplete_or_conflicting_evidence", evidence
    )


def probe_execution_liveness(
    lease: ExecutionLeaseSnapshot | None,
    *,
    probe: ExecutionLivenessProbe | None = None,
) -> ExecutionLivenessDecision:
    """Collect and decide one transcript; every probe exception is unknown."""
    if lease is None:
        return decide_execution_liveness(ExecutionLivenessEvidence(
            lease=None,
            current_host_boot_id=None,
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        ))
    selected_probe = SystemExecutionLivenessProbe() if probe is None else probe
    try:
        evidence = selected_probe.observe(lease)
    except Exception as exc:  # noqa: BLE001 - probe boundary fails closed
        evidence = ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=None,
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
            probe_error=f"{type(exc).__name__}: {exc}",
        )
    return decide_execution_liveness(evidence)


class SystemExecutionLivenessProbe:
    """Linux procfs + systemd implementation of the shared liveness probe."""

    def __init__(
        self,
        *,
        proc_root: pathlib.Path = pathlib.Path("/proc"),
        boot_id_path: pathlib.Path = pathlib.Path(
            "/proc/sys/kernel/random/boot_id"
        ),
        cgroup_root: pathlib.Path = pathlib.Path("/sys/fs/cgroup"),
    ) -> None:
        self.proc_root = proc_root
        self.boot_id_path = boot_id_path
        self.cgroup_root = cgroup_root

    def _process(self, identity: ProcessIdentity) -> ProcessObservation:
        try:
            observed_ticks = read_process_start_ticks(
                identity.pid,
                proc_root=self.proc_root,
            )
        except FileNotFoundError:
            return ProcessObservation(
                identity, "absent", None, None, "pid_absent"
            )
        except OSError as exc:
            return ProcessObservation(
                identity,
                "unknown",
                None,
                None,
                f"pid_probe_failed:{type(exc).__name__}",
            )
        if observed_ticks != identity.start_ticks:
            return ProcessObservation(
                identity,
                "reused",
                observed_ticks,
                None,
                "pid_reused",
            )
        try:
            cgroup_path = self._process_cgroup(identity.pid)
        except OSError as exc:
            return ProcessObservation(
                identity,
                "unknown",
                observed_ticks,
                None,
                f"process_cgroup_failed:{type(exc).__name__}",
            )
        return ProcessObservation(
            identity,
            "exact",
            observed_ticks,
            cgroup_path,
            "exact_process_identity",
        )

    def _process_cgroup(self, pid: int) -> str:
        rows = (self.proc_root / str(pid) / "cgroup").read_text().splitlines()
        fallback: str | None = None
        for row in rows:
            parts = row.split(":", 2)
            if len(parts) != 3:
                raise OSError(f"malformed /proc/{pid}/cgroup")
            hierarchy, controllers, path = parts
            if hierarchy == "0" and controllers == "":
                return path
            if "name=systemd" in controllers.split(","):
                return path
            if fallback is None:
                fallback = path
        if fallback is None:
            raise OSError(f"empty /proc/{pid}/cgroup")
        return fallback

    @staticmethod
    def _invocation(
        lease: ExecutionLeaseSnapshot,
    ) -> InvocationObservation:
        completed = subprocess.run(
            [
                "systemctl",
                "show",
                lease.systemd_unit,
                "--property=InvocationID",
                "--property=ControlGroup",
                "--property=ActiveState",
                "--property=SubState",
                "--no-pager",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=5,
        )
        if completed.returncode != 0:
            return InvocationObservation(
                "unknown",
                lease.invocation_id,
                None,
                None,
                f"systemctl_failed:{completed.returncode}",
            )
        properties: dict[str, str] = {}
        for row in completed.stdout.splitlines():
            key, separator, value = row.partition("=")
            if separator:
                properties[key] = value
        required = {"InvocationID", "ControlGroup", "ActiveState", "SubState"}
        if not required.issubset(properties):
            return InvocationObservation(
                "unknown",
                lease.invocation_id,
                None,
                None,
                "systemctl_properties_missing",
            )
        invocation_id = properties["InvocationID"].strip() or None
        control_group = properties["ControlGroup"].strip() or None
        active_state = properties["ActiveState"].strip()
        sub_state = properties["SubState"].strip()
        if not active_state or not sub_state:
            return InvocationObservation(
                "unknown",
                lease.invocation_id,
                invocation_id,
                control_group,
                "systemctl_state_blank",
            )
        inactive = active_state in {"inactive", "failed"}
        if invocation_id is None and inactive:
            state: InvocationState = "ended"
            reason = "invocation_ended"
        elif invocation_id is None:
            state = "unknown"
            reason = "active_unit_missing_invocation"
        elif invocation_id == lease.invocation_id:
            if inactive:
                state = "ended"
                reason = "retained_invocation_inactive"
            else:
                state = "exact"
                reason = "exact_invocation"
        else:
            # A unit can only have one current InvocationID. A newer ID proves
            # the stored invocation ended; it is not conflicting evidence
            # about the old execution.
            state = "ended"
            reason = "stored_invocation_replaced"
        return InvocationObservation(
            state,
            lease.invocation_id,
            invocation_id,
            control_group,
            reason,
            active_state,
            sub_state,
        )

    def _cgroup(
        self,
        invocation: InvocationObservation,
        processes: tuple[ProcessObservation, ...],
    ) -> CgroupObservation:
        if invocation.state == "unknown":
            return CgroupObservation(
                "unknown", invocation.control_group, (), invocation.reason
            )
        if invocation.state == "conflict":
            return CgroupObservation(
                "conflict", invocation.control_group, (), invocation.reason
            )
        if invocation.state == "exact":
            path = invocation.control_group
            if path is None:
                return CgroupObservation(
                    "unknown", None, (), "live_invocation_has_no_cgroup"
                )
            exact = tuple(
                process for process in processes if process.state == "exact"
            )
            if any(
                process.cgroup_path != path
                and not (
                    process.cgroup_path is not None
                    and process.cgroup_path.startswith(path.rstrip("/") + "/")
                )
                for process in exact
            ):
                return CgroupObservation(
                    "conflict",
                    path,
                    tuple(process.identity.pid for process in exact),
                    "exact_process_outside_unit_cgroup",
                )
            if exact:
                return CgroupObservation(
                    "exact",
                    path,
                    tuple(process.identity.pid for process in exact),
                    "exact_process_in_unit_cgroup",
                )
            try:
                raw_pids = (
                    self.cgroup_root
                    / path.lstrip("/")
                    / "cgroup.procs"
                ).read_text().split()
                members = tuple(int(pid) for pid in raw_pids)
            except (OSError, ValueError) as exc:
                return CgroupObservation(
                    "unknown",
                    path,
                    (),
                    f"live_cgroup_probe_failed:{type(exc).__name__}",
                )
            if not members:
                return CgroupObservation(
                    "unknown", path, (), "live_cgroup_empty"
                )
            return CgroupObservation(
                "exact",
                path,
                members,
                "exact_unit_cgroup",
            )

        path = invocation.control_group
        if (
            invocation.observed_invocation_id is not None
            and invocation.observed_invocation_id
            != invocation.stored_invocation_id
        ):
            return CgroupObservation(
                "absent", path, (), "stored_cgroup_replaced"
            )
        if path is None:
            return CgroupObservation(
                "absent", None, (), "ended_invocation_has_no_cgroup"
            )
        try:
            raw_pids = (self.cgroup_root / path.lstrip("/") / "cgroup.procs"
                        ).read_text().split()
            members = tuple(int(pid) for pid in raw_pids)
        except FileNotFoundError:
            return CgroupObservation(
                "absent", path, (), "ended_cgroup_absent"
            )
        except (OSError, ValueError) as exc:
            return CgroupObservation(
                "unknown",
                path,
                (),
                f"ended_cgroup_probe_failed:{type(exc).__name__}",
            )
        if members:
            return CgroupObservation(
                "conflict", path, members, "ended_cgroup_still_populated"
            )
        return CgroupObservation(
            "absent", path, (), "ended_cgroup_empty"
        )

    def observe(
        self,
        lease: ExecutionLeaseSnapshot,
    ) -> ExecutionLivenessEvidence:
        try:
            boot_id = _read_nonblank(self.boot_id_path, "host boot ID")
        except OSError as exc:
            return ExecutionLivenessEvidence(
                lease=lease,
                current_host_boot_id=None,
                boot_error=f"{type(exc).__name__}: {exc}",
                worker=None,
                beets=None,
                invocation=None,
                cgroup=None,
            )
        if boot_id != lease.host_boot_id:
            return ExecutionLivenessEvidence(
                lease=lease,
                current_host_boot_id=boot_id,
                boot_error=None,
                worker=None,
                beets=None,
                invocation=None,
                cgroup=None,
            )
        worker = self._process(lease.worker)
        beets = self._process(lease.beets) if lease.beets is not None else None
        invocation = self._invocation(lease)
        cgroup = self._cgroup(
            invocation,
            tuple(
                process
                for process in (worker, beets)
                if process is not None
            ),
        )
        return ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id=boot_id,
            boot_error=None,
            worker=worker,
            beets=beets,
            invocation=invocation,
            cgroup=cgroup,
        )


class AutomationOwnerFailStop(RuntimeError):
    """End a worker whose live lease still owns unfinished automation work.

    The worker must exit so systemd can restart it and the shared liveness
    service can prove the persisted execution dead before automatic recovery.
    Returning to the daemon loop would keep the lease live and strand the
    request in ``processing``.
    """


class ExecutionCancelled(RuntimeError):
    """Raised before the next mutation after execution cancellation."""


class CancellationToken:
    """Thread-safe, one-way cancellation with first-reason evidence."""

    def __init__(self) -> None:
        self._event = threading.Event()
        self._reason_lock = threading.Lock()
        self._reason: str | None = None

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    @property
    def reason(self) -> str | None:
        with self._reason_lock:
            return self._reason

    def cancel(self, reason: str) -> bool:
        if not reason.strip():
            raise ValueError("cancellation reason must be nonblank")
        with self._reason_lock:
            if self._reason is not None:
                return False
            self._reason = reason
            self._event.set()
            return True

    def raise_if_cancelled(self) -> None:
        reason = self.reason
        if reason is not None:
            raise ExecutionCancelled(reason)


def checkpoint(cancellation_token: CancellationToken | None) -> None:
    """Refuse to continue when execution has been cancelled.

    An uncancellable caller passes ``None`` and this does nothing, which is
    why every stage can call it unconditionally. Five modules carried a
    byte-identical private copy of this before issue #1313 folded them here.
    """
    if cancellation_token is not None:
        cancellation_token.raise_if_cancelled()


def cancellation_hook(
    cancellation_token: CancellationToken | None,
) -> Callable[[], None] | None:
    """The ``before_mutation`` hook a token implies, or ``None`` without one.

    Filesystem-authority mutators (``lib.fs_authority.remove_relative_tree``,
    ``lib.import_manifest``'s movers) take ``before_mutation`` and re-check it
    immediately before each irreversible step. Translating a token into that
    hook is the same two lines everywhere, so it is written once here rather
    than as the four private ``*_cancellable`` wrappers and one inline
    conditional that used to spell it (issue #1313).
    """
    if cancellation_token is None:
        return None
    return cancellation_token.raise_if_cancelled


class OwnerSessionWatchdog:
    """Probe one pinned owner session throughout a non-database stage."""

    def __init__(
        self,
        token: CancellationToken,
        probe: Callable[[float], bool],
        *,
        probe_interval: float = 1.0,
    ) -> None:
        if not 0 < probe_interval <= 1.0:
            raise ValueError("probe_interval must be in (0, 1.0]")
        self._token = token
        self._probe = probe
        self._probe_interval = probe_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _probe_once(self) -> None:
        if self._token.cancelled:
            return
        try:
            live = self._probe(self._probe_interval)
        except Exception as exc:  # noqa: BLE001 - fail-stop boundary
            self._token.cancel(
                f"owner_session_probe_failed:{type(exc).__name__}"
            )
            return
        if not live:
            self._token.cancel("owner_session_lost")

    def _run(self) -> None:
        while not self._stop.is_set():
            next_probe_at = time.monotonic() + self._probe_interval
            self._probe_once()
            if self._token.cancelled:
                return
            self._stop.wait(max(0.0, next_probe_at - time.monotonic()))

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("owner-session watchdog already started")
        self._thread = threading.Thread(
            target=self._run,
            name="import-owner-session-watchdog",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(
        self,
        _exc_type: object,
        _exc_value: object,
        _traceback: object,
    ) -> None:
        self.stop()


class ProcessGroupTerminationError(RuntimeError):
    """The spawned process group could not be proven absent by its deadline."""


class MonitoredProcessGroup:
    """A ``start_new_session`` child whose entire process group is cancellable."""

    def __init__(self, process: subprocess.Popen[bytes]) -> None:
        self._process = process
        self._termination_lock = threading.Lock()
        self._terminated_returncode: int | None = None

    @property
    def pid(self) -> int:
        return self._process.pid

    @property
    def returncode(self) -> int | None:
        return self._process.poll()

    def _group_exists(self) -> bool:
        try:
            os.killpg(self._process.pid, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _reap_leader_nonblocking(self) -> int | None:
        return self._process.poll()

    def _wait_until(
        self,
        *,
        deadline: float,
        require_group_absent: bool,
    ) -> bool:
        while time.monotonic() < deadline:
            self._reap_leader_nonblocking()
            if not require_group_absent or not self._group_exists():
                return True
            remaining = deadline - time.monotonic()
            if remaining > 0:
                threading.Event().wait(min(0.01, remaining))
        self._reap_leader_nonblocking()
        return not require_group_absent or not self._group_exists()

    def terminate_and_wait(self, *, timeout: float = 5.0) -> int:
        """Terminate, reap, and prove absence of the complete child group."""
        if timeout <= 0:
            raise ValueError("process-group termination timeout must be positive")
        with self._termination_lock:
            if self._terminated_returncode is not None:
                return self._terminated_returncode
            deadline = time.monotonic() + timeout
            try:
                os.killpg(self._process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            term_deadline = min(
                deadline,
                time.monotonic() + min(1.0, timeout / 2),
            )
            if not self._wait_until(
                deadline=term_deadline,
                require_group_absent=True,
            ):
                try:
                    os.killpg(self._process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            if not self._wait_until(
                deadline=deadline,
                require_group_absent=True,
            ):
                raise ProcessGroupTerminationError(
                    "process group "
                    f"{self._process.pid} survived termination deadline"
                )
            remaining = max(0.0, deadline - time.monotonic())
            try:
                returncode = self._process.wait(timeout=remaining)
            except subprocess.TimeoutExpired as exc:
                raise ProcessGroupTerminationError(
                    "process-group leader "
                    f"{self._process.pid} was not reaped by termination deadline"
                ) from exc
            self._terminated_returncode = returncode
            return returncode

    def wait(
        self,
        token: CancellationToken,
        *,
        owner_session_probe: Callable[[], bool] | None = None,
        probe_interval: float = 1.0,
    ) -> int:
        """Wait while checking cancellation/session health at least once/sec."""
        if not 0 < probe_interval <= 1.0:
            raise ValueError("probe_interval must be in (0, 1.0]")
        try:
            while True:
                token.raise_if_cancelled()
                if owner_session_probe is not None:
                    live = False
                    try:
                        live = owner_session_probe()
                    except Exception as exc:  # noqa: BLE001 - fail-stop boundary
                        token.cancel(
                            f"owner_session_probe_failed:{type(exc).__name__}"
                        )
                        token.raise_if_cancelled()
                    if not live:
                        token.cancel("owner_session_lost")
                        token.raise_if_cancelled()
                try:
                    returncode = self._process.wait(
                        timeout=probe_interval
                    )
                    # Cancellation may race the child's exit. It wins: reap
                    # and prove the whole group absent before propagating it.
                    token.raise_if_cancelled()
                    return returncode
                except subprocess.TimeoutExpired:
                    continue
        except ExecutionCancelled:
            self.terminate_and_wait()
            raise


@dataclass(frozen=True)
class OwnerSessionIdentity:
    """Stable identity of the exact PostgreSQL session holding IMPORT."""

    connection_object_id: int
    backend_pid: int


@dataclass(frozen=True)
class ExecutionOwnerProof:
    """The #898 exact-owner pair, threaded together through one lifecycle.

    ``execution_lease`` and ``owner_session_identity`` are supplied or
    omitted together everywhere the completed-download lifecycle
    (``lib.download_processing``, ``lib.download``,
    ``lib.download_validation``) re-threads them — no call site there has
    ever passed one without the other. Bundling them turns that co-nullity
    from an unenforced convention into a type: a caller cannot construct a
    request that reverifies liveness without also naming the session it
    expects to still hold ``IMPORT``.

    Deliberately excludes ``cancellation_token``: several reject paths in
    that same lifecycle carry a token with no owner proof at all, so a
    three-way bundle would force those call sites to name an ownership pair
    they never use.
    """

    execution_lease: ExecutionLeaseSnapshot
    owner_session_identity: OwnerSessionIdentity


@dataclass(frozen=True)
class OwnerSessionProbe:
    live: bool
    reason: str
    expected: OwnerSessionIdentity
    observed_backend_pid: int | None


class AutomationOwnerCheckpointDB(Protocol):
    """Persistence seam for one exact automation-owner checkpoint."""

    def _probe_owner_session(
        self,
        identity: OwnerSessionIdentity,
    ) -> OwnerSessionProbe: ...

    def heartbeat_import_job(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
    ) -> bool: ...

    def heartbeat_import_job_preview(
        self,
        job_id: int,
        *,
        expected_execution_lease: ExecutionLeaseSnapshot,
    ) -> bool: ...


def checkpoint_automation_owner(
    db: AutomationOwnerCheckpointDB,
    *,
    import_job_id: int,
    execution_lease: ExecutionLeaseSnapshot,
    cancellation_token: CancellationToken,
    owner_session_identity: OwnerSessionIdentity,
) -> None:
    """Fail before the next effect unless the exact owner/session is live."""
    cancellation_token.raise_if_cancelled()
    probe = db._probe_owner_session(owner_session_identity)
    if not probe.live:
        cancellation_token.cancel(
            f"owner_session_reverification_failed:{probe.reason}"
        )
        cancellation_token.raise_if_cancelled()
    if db.heartbeat_import_job(
        import_job_id,
        expected_execution_lease=execution_lease,
    ):
        return
    if db.heartbeat_import_job_preview(
        import_job_id,
        expected_execution_lease=execution_lease,
    ):
        return
    cancellation_token.cancel("automation_import_owner_changed")
    cancellation_token.raise_if_cancelled()
