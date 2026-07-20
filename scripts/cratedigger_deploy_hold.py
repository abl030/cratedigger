#!/usr/bin/env python3
"""Acquire and release Cratedigger's strict systemd deployment hold.

The fixed unit set and root-owned runtime receipt are deliberate. This helper
never accepts arbitrary unit names, never masks a service, and never removes a
control link it did not create and record itself.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
from typing import Protocol


CONTROL_DIR = "/run/systemd/system.control"
STATE_DIR = Path("/run/cratedigger-deploy-hold")
STATE_STAGING_DIR = Path("/run/.cratedigger-deploy-hold.creating")
STATE_RETIRED_DIR = Path("/run/.cratedigger-deploy-hold.retired")
METADATA_MANUAL_HOLD = Path("/run/cratedigger-metadata-gate/holds/manual")

MAIN_TIMER = "cratedigger.timer"
UNFINDABLE_TIMER = "cratedigger-unfindable.timer"
WATCHDOG_TIMER = "cratedigger-metadata-gate-watchdog.timer"
TIMER_UNITS = (MAIN_TIMER, UNFINDABLE_TIMER, WATCHDOG_TIMER)

MAIN_SERVICE = "cratedigger.service"
UNFINDABLE_SERVICE = "cratedigger-unfindable.service"
WATCHDOG_SERVICE = "cratedigger-metadata-gate-watchdog.service"
SERVICE_UNITS = (MAIN_SERVICE, UNFINDABLE_SERVICE, WATCHDOG_SERVICE)

PHASE_ACQUIRING = "acquiring"
PHASE_HELD = "held"
PHASE_PREPARED_CONTROLLED = "prepared-controlled"
PHASE_MAIN_TIMER_OPEN = "main-timer-open"
PHASE_COMPLETE_PENDING = "complete-pending"

_RECEIPT_VERSION = "cratedigger-deploy-hold-v1"
_RECEIPT_FILE = "receipt"
_PHASE_FILE = "phase"
_MANUAL_MARKER = "owned-manual-hold"
_LINK_MARKER_PREFIX = "owned-link-"
_INVOCATION_FILE = "ordinary-invocation"
_INVOCATION_RE = re.compile(r"[0-9a-f]{32}")
_DRAIN_TIMEOUT_SECONDS = 7200.0
_POLL_SECONDS = 1.0
_STABLE_SAMPLES = 2


class DeployHoldError(RuntimeError):
    """The strict hold could not prove the requested lifecycle boundary."""


@dataclass(frozen=True)
class UnitState:
    load_state: str
    active_state: str
    sub_state: str


@dataclass(frozen=True)
class JobState:
    job_id: str
    unit: str
    job_type: str
    state: str

    @classmethod
    def none(cls) -> JobState:
        return cls(job_id="", unit="", job_type="", state="")


class DeployHoldBackend(Protocol):
    def ensure_control_dir(self) -> None: ...
    def receipt_exists(self) -> bool: ...
    def retired_receipt_exists(self) -> bool: ...
    def create_receipt(self) -> None: ...
    def remove_receipt(self) -> None: ...
    def finish_retired_receipt(self) -> None: ...
    def read_phase(self) -> str: ...
    def write_phase(self, phase: str) -> None: ...
    def mark_manual_hold_owned(self) -> None: ...
    def unmark_manual_hold_owned(self) -> None: ...
    def manual_hold_is_owned(self) -> bool: ...
    def mark_link_owned(self, timer: str) -> None: ...
    def unmark_link_owned(self, timer: str) -> None: ...
    def link_is_owned(self, timer: str) -> bool: ...
    def owned_link_units(self) -> tuple[str, ...]: ...
    def write_ordinary_invocation(self, invocation_id: str) -> None: ...
    def read_ordinary_invocation(self) -> str: ...
    def clear_ordinary_invocation(self) -> None: ...
    def manual_hold_active(self) -> bool: ...
    def metadata_gate(self, command: str) -> None: ...
    def control_link_target(self, timer: str) -> str | None: ...
    def create_control_mask(self, timer: str) -> None: ...
    def remove_control_mask(self, timer: str) -> None: ...
    def daemon_reload(self) -> None: ...
    def stop_units(self, units: Iterable[str]) -> None: ...
    def start_unit(self, unit: str) -> None: ...
    def unit_state(self, unit: str) -> UnitState: ...
    def job_state(self, unit: str) -> JobState: ...
    def cancel_job(self, job_id: str) -> None: ...
    def reset_failed(self, unit: str) -> None: ...
    def monotonic(self) -> float: ...
    def sleep(self, seconds: float) -> None: ...


class RealSystemdBackend:
    """Root-local backend for systemd, metadata-gate, and receipt state."""

    def _run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(argv),
            check=check,
            capture_output=True,
            text=True,
        )

    def ensure_control_dir(self) -> None:
        path = Path(CONTROL_DIR)
        try:
            path.mkdir(mode=0o755)
        except FileExistsError:
            pass
        info = path.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != 0
            or stat.S_IMODE(info.st_mode) != 0o755
        ):
            raise DeployHoldError(
                "systemd control directory is not a root-owned mode-0755 directory"
            )

    @staticmethod
    def _validate_unit(unit: str, allowed: tuple[str, ...]) -> None:
        if unit not in allowed:
            raise DeployHoldError(f"unit outside fixed hold scope: {unit}")

    @staticmethod
    def _marker_path(name: str) -> Path:
        if not name or "/" in name or name in {".", ".."}:
            raise DeployHoldError(f"invalid receipt marker: {name!r}")
        return STATE_DIR / name

    @staticmethod
    def _validate_private_dir(path: Path, description: str) -> None:
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise DeployHoldError(f"{description} is missing") from exc
        if not stat.S_ISDIR(info.st_mode) or stat.S_ISLNK(info.st_mode):
            raise DeployHoldError(f"{description} is not a directory")
        if info.st_uid != 0 or stat.S_IMODE(info.st_mode) != 0o700:
            raise DeployHoldError(f"{description} is not root-owned mode 0700")

    @classmethod
    def _clear_reserved_dir(
        cls,
        path: Path,
        *,
        allowed: set[str],
        description: str,
    ) -> None:
        cls._validate_private_dir(path, description)
        entries = tuple(path.iterdir())
        unexpected = {entry.name for entry in entries} - allowed
        if unexpected:
            raise DeployHoldError(
                f"{description} has unknown entries: {sorted(unexpected)!r}"
            )
        for entry in entries:
            info = entry.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise DeployHoldError(
                    f"{description} entry is not a root-owned mode-0600 file: "
                    f"{entry.name}"
                )
        for entry in entries:
            entry.unlink()
        path.rmdir()

    @staticmethod
    def _write_new_file(directory: Path, name: str, value: str) -> None:
        target = directory / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        descriptor = os.open(target, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    @staticmethod
    def _validate_state_dir() -> None:
        RealSystemdBackend._validate_private_dir(
            STATE_DIR,
            "deploy hold receipt",
        )

    @classmethod
    def _read_marker(cls, name: str) -> str:
        cls._validate_state_dir()
        path = cls._marker_path(name)
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise DeployHoldError(f"receipt marker is missing: {name}") from exc
        if not stat.S_ISREG(info.st_mode) or info.st_uid != 0:
            raise DeployHoldError(f"receipt marker is not a root-owned file: {name}")
        if stat.S_IMODE(info.st_mode) != 0o600:
            raise DeployHoldError(f"receipt marker has unsafe mode: {name}")
        return path.read_text(encoding="utf-8").rstrip("\n")

    @classmethod
    def _write_marker(cls, name: str, value: str, *, replace: bool) -> None:
        cls._validate_state_dir()
        target = cls._marker_path(name)
        if not replace:
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
            try:
                descriptor = os.open(target, flags, 0o600)
            except FileExistsError as exc:
                raise DeployHoldError(f"receipt marker already exists: {name}") from exc
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                handle.write(value + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return

        temp_path = cls._marker_path(f".next-{name}")
        if os.path.lexists(temp_path):
            info = temp_path.lstat()
            if (
                not stat.S_ISREG(info.st_mode)
                or info.st_uid != 0
                or stat.S_IMODE(info.st_mode) != 0o600
            ):
                raise DeployHoldError(
                    f"replacement marker has unsafe state: {temp_path.name}"
                )
            temp_path.unlink()
        try:
            cls._write_new_file(STATE_DIR, temp_path.name, value)
            os.replace(temp_path, target)
        finally:
            if os.path.lexists(temp_path):
                temp_path.unlink()

    @classmethod
    def _remove_marker(cls, name: str) -> None:
        cls._read_marker(name)
        cls._marker_path(name).unlink()

    def receipt_exists(self) -> bool:
        return os.path.lexists(STATE_DIR)

    def retired_receipt_exists(self) -> bool:
        return os.path.lexists(STATE_RETIRED_DIR)

    def create_receipt(self) -> None:
        if self.retired_receipt_exists():
            raise DeployHoldError(
                "retired deploy receipt needs cleanup; rerun the interrupted complete"
            )
        if os.path.lexists(STATE_STAGING_DIR):
            self._clear_reserved_dir(
                STATE_STAGING_DIR,
                allowed={_RECEIPT_FILE, _PHASE_FILE},
                description="deploy hold staging receipt",
            )
        try:
            STATE_STAGING_DIR.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise DeployHoldError("deploy hold staging receipt already exists") from exc
        try:
            self._write_new_file(
                STATE_STAGING_DIR,
                _RECEIPT_FILE,
                _RECEIPT_VERSION,
            )
            self._write_new_file(
                STATE_STAGING_DIR,
                _PHASE_FILE,
                PHASE_ACQUIRING,
            )
            if os.path.lexists(STATE_DIR):
                raise DeployHoldError("deploy hold receipt already exists")
            os.rename(STATE_STAGING_DIR, STATE_DIR)
        except BaseException:
            if os.path.lexists(STATE_STAGING_DIR):
                self._clear_reserved_dir(
                    STATE_STAGING_DIR,
                    allowed={_RECEIPT_FILE, _PHASE_FILE},
                    description="deploy hold staging receipt",
                )
            raise

    def remove_receipt(self) -> None:
        self._validate_receipt()
        allowed = {_RECEIPT_FILE, _PHASE_FILE, _INVOCATION_FILE}
        entries = {entry.name for entry in STATE_DIR.iterdir()}
        unexpected = entries - allowed
        if unexpected:
            raise DeployHoldError(
                f"receipt still has owned or unknown markers: {sorted(unexpected)!r}"
            )
        if self.retired_receipt_exists():
            raise DeployHoldError("a retired deploy receipt already exists")
        os.rename(STATE_DIR, STATE_RETIRED_DIR)
        self.finish_retired_receipt()

    def finish_retired_receipt(self) -> None:
        self._clear_reserved_dir(
            STATE_RETIRED_DIR,
            allowed={_RECEIPT_FILE, _PHASE_FILE, _INVOCATION_FILE},
            description="retired deploy hold receipt",
        )

    def _validate_receipt(self) -> None:
        if self._read_marker(_RECEIPT_FILE) != _RECEIPT_VERSION:
            raise DeployHoldError("deploy hold receipt has unknown ownership marker")

    def read_phase(self) -> str:
        self._validate_receipt()
        return self._read_marker(_PHASE_FILE)

    def write_phase(self, phase: str) -> None:
        self._validate_receipt()
        self._write_marker(_PHASE_FILE, phase, replace=True)

    def mark_manual_hold_owned(self) -> None:
        self._write_marker(_MANUAL_MARKER, "manual", replace=False)

    def unmark_manual_hold_owned(self) -> None:
        if self._read_marker(_MANUAL_MARKER) != "manual":
            raise DeployHoldError("manual hold ownership marker changed")
        self._marker_path(_MANUAL_MARKER).unlink()

    def manual_hold_is_owned(self) -> bool:
        path = self._marker_path(_MANUAL_MARKER)
        if not path.exists():
            return False
        return self._read_marker(_MANUAL_MARKER) == "manual"

    @staticmethod
    def _link_marker(timer: str) -> str:
        RealSystemdBackend._validate_unit(timer, TIMER_UNITS)
        return _LINK_MARKER_PREFIX + timer

    def mark_link_owned(self, timer: str) -> None:
        self._write_marker(self._link_marker(timer), timer, replace=False)

    def unmark_link_owned(self, timer: str) -> None:
        marker = self._link_marker(timer)
        if self._read_marker(marker) != timer:
            raise DeployHoldError(f"control-link ownership marker changed: {timer}")
        self._marker_path(marker).unlink()

    def link_is_owned(self, timer: str) -> bool:
        marker_name = self._link_marker(timer)
        marker = self._marker_path(marker_name)
        if not marker.exists():
            return False
        return self._read_marker(marker_name) == timer

    def owned_link_units(self) -> tuple[str, ...]:
        self._validate_receipt()
        owned: list[str] = []
        for entry in STATE_DIR.iterdir():
            if not entry.name.startswith(_LINK_MARKER_PREFIX):
                continue
            timer = entry.name.removeprefix(_LINK_MARKER_PREFIX)
            self._validate_unit(timer, TIMER_UNITS)
            if self._read_marker(entry.name) != timer:
                raise DeployHoldError(f"control-link ownership marker changed: {timer}")
            owned.append(timer)
        return tuple(sorted(owned))

    def write_ordinary_invocation(self, invocation_id: str) -> None:
        self._write_marker(_INVOCATION_FILE, invocation_id, replace=False)

    def read_ordinary_invocation(self) -> str:
        return self._read_marker(_INVOCATION_FILE)

    def clear_ordinary_invocation(self) -> None:
        path = self._marker_path(_INVOCATION_FILE)
        if os.path.lexists(path):
            self._remove_marker(_INVOCATION_FILE)

    def manual_hold_active(self) -> bool:
        return METADATA_MANUAL_HOLD.exists()

    def metadata_gate(self, command: str) -> None:
        commands = {
            "hold manual": ("hold", "manual"),
            "release manual": ("release", "manual"),
            "resume-if-clear": ("resume-if-clear",),
        }
        args = commands.get(command)
        if args is None:
            raise DeployHoldError(f"metadata-gate command outside fixed scope: {command}")
        self._run(("cratedigger-metadata-gate", *args))

    @staticmethod
    def _control_path(timer: str) -> Path:
        RealSystemdBackend._validate_unit(timer, TIMER_UNITS)
        return Path(CONTROL_DIR) / timer

    def control_link_target(self, timer: str) -> str | None:
        path = self._control_path(timer)
        try:
            info = path.lstat()
        except FileNotFoundError:
            return None
        if not stat.S_ISLNK(info.st_mode):
            return "<not-a-symlink>"
        return os.readlink(path)

    def create_control_mask(self, timer: str) -> None:
        os.symlink("/dev/null", self._control_path(timer))

    def remove_control_mask(self, timer: str) -> None:
        self._control_path(timer).unlink()

    def daemon_reload(self) -> None:
        self._run(("systemctl", "daemon-reload"))

    def stop_units(self, units: Iterable[str]) -> None:
        exact = tuple(units)
        if not exact or any(unit not in TIMER_UNITS for unit in exact):
            raise DeployHoldError(f"stop outside fixed timer scope: {exact!r}")
        self._run(("systemctl", "stop", *exact))

    def start_unit(self, unit: str) -> None:
        self._validate_unit(unit, (*TIMER_UNITS, MAIN_SERVICE))
        args = (
            ("systemctl", "start", "--no-block", unit)
            if unit == MAIN_SERVICE
            else ("systemctl", "start", unit)
        )
        self._run(args)

    @staticmethod
    def _parse_properties(output: str, expected: tuple[str, ...]) -> dict[str, str]:
        values: dict[str, str] = {}
        for line in output.splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in expected or key in values:
                raise DeployHoldError(f"unexpected systemctl property line: {line!r}")
            values[key] = value
        if set(values) != set(expected):
            raise DeployHoldError(
                f"missing systemctl properties: {sorted(set(expected) - set(values))!r}"
            )
        return values

    def unit_state(self, unit: str) -> UnitState:
        self._validate_unit(unit, (*TIMER_UNITS, *SERVICE_UNITS))
        expected = ("LoadState", "ActiveState", "SubState")
        proc = self._run((
            "systemctl",
            "show",
            unit,
            "--property=LoadState",
            "--property=ActiveState",
            "--property=SubState",
        ))
        values = self._parse_properties(proc.stdout, expected)
        return UnitState(
            load_state=values["LoadState"],
            active_state=values["ActiveState"],
            sub_state=values["SubState"],
        )

    def job_state(self, unit: str) -> JobState:
        self._validate_unit(unit, SERVICE_UNITS)
        for _attempt in range(3):
            job_proc = self._run(
                ("systemctl", "show", unit, "--property=Job", "--value")
            )
            job_id = job_proc.stdout.strip()
            if not job_id or job_id == "0":
                return JobState.none()
            if not job_id.isdecimal():
                raise DeployHoldError(f"invalid systemd job id for {unit}: {job_id!r}")
            expected = ("Id", "Unit", "JobType", "State")
            detail = self._run(
                (
                    "systemctl",
                    "show",
                    job_id,
                    "--property=Id",
                    "--property=Unit",
                    "--property=JobType",
                    "--property=State",
                ),
                check=False,
            )
            if detail.returncode != 0:
                continue
            values = self._parse_properties(detail.stdout, expected)
            return JobState(
                job_id=values["Id"],
                unit=values["Unit"],
                job_type=values["JobType"],
                state=values["State"],
            )
        raise DeployHoldError(f"systemd job for {unit} changed during inspection")

    def cancel_job(self, job_id: str) -> None:
        if not job_id.isdecimal():
            raise DeployHoldError(f"refusing non-numeric systemd job id: {job_id!r}")
        self._run(("systemctl", "cancel", job_id))

    def reset_failed(self, unit: str) -> None:
        self._validate_unit(unit, SERVICE_UNITS)
        self._run(("systemctl", "reset-failed", unit))

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


def _require_phase(backend: DeployHoldBackend, expected: str) -> None:
    if not backend.receipt_exists():
        raise DeployHoldError("deploy hold receipt is missing")
    actual = backend.read_phase()
    if actual != expected:
        raise DeployHoldError(f"expected phase {expected!r}, found {actual!r}")


def _validate_invocation_id(invocation_id: str) -> None:
    if _INVOCATION_RE.fullmatch(invocation_id) is None:
        raise DeployHoldError(
            f"InvocationID must be exactly 32 lowercase hexadecimal characters: "
            f"{invocation_id!r}"
        )


def _assert_owned_links(
    backend: DeployHoldBackend,
    expected_units: tuple[str, ...],
) -> None:
    actual = set(backend.owned_link_units())
    expected = set(expected_units)
    if actual != expected:
        raise DeployHoldError(
            f"owned control-link set changed: expected {sorted(expected)!r}, "
            f"found {sorted(actual)!r}"
        )
    for timer in expected_units:
        if not backend.link_is_owned(timer):
            raise DeployHoldError(f"control link is not receipt-owned: {timer}")
        target = backend.control_link_target(timer)
        if target != "/dev/null":
            raise DeployHoldError(
                f"owned control link changed for {timer}: {target!r}"
            )


def _assert_load_states(
    backend: DeployHoldBackend,
    *,
    masked: tuple[str, ...],
    loaded: tuple[str, ...],
) -> None:
    for timer in masked:
        state = backend.unit_state(timer)
        if state.load_state != "masked":
            raise DeployHoldError(
                f"authoritative timer mask failed for {timer}: "
                f"LoadState={state.load_state}"
            )
    for timer in loaded:
        state = backend.unit_state(timer)
        if state.load_state != "loaded":
            raise DeployHoldError(
                f"timer did not restore as loaded for {timer}: "
                f"LoadState={state.load_state}"
            )


def _drain_owned_services(backend: DeployHoldBackend) -> None:
    deadline = backend.monotonic() + _DRAIN_TIMEOUT_SECONDS
    stable_samples = 0
    while backend.monotonic() < deadline:
        safe = True
        for service in SERVICE_UNITS:
            job = backend.job_state(service)
            if job != JobState.none():
                if job.unit != service:
                    raise DeployHoldError(
                        f"job {job.job_id} changed unit during inspection: "
                        f"{job.unit!r} != {service!r}"
                    )
                if job.job_type == "start" and job.state == "waiting":
                    backend.cancel_job(job.job_id)
                safe = False
            state = backend.unit_state(service)
            if job == JobState.none() and (
                state.active_state,
                state.sub_state,
            ) == ("failed", "failed"):
                backend.reset_failed(service)
                safe = False
                continue
            if (state.active_state, state.sub_state) != ("inactive", "dead"):
                safe = False
        if safe:
            stable_samples += 1
            if stable_samples >= _STABLE_SAMPLES:
                return
        else:
            stable_samples = 0
        backend.sleep(_POLL_SECONDS)
    raise DeployHoldError(
        "timed out waiting for exact services to become stably inactive and job-free"
    )


def _verify_authoritative_hold(backend: DeployHoldBackend) -> None:
    if not backend.manual_hold_is_owned() or not backend.manual_hold_active():
        raise DeployHoldError("receipt-owned manual metadata hold is not active")
    _assert_owned_links(backend, TIMER_UNITS)
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    backend.stop_units(TIMER_UNITS)
    _drain_owned_services(backend)


def _ensure_owned_manual_hold(backend: DeployHoldBackend) -> None:
    if not backend.manual_hold_is_owned():
        if backend.manual_hold_active():
            raise DeployHoldError("unowned manual hold appeared during acquisition")
        # Record intent before mutation so an interrupted acquire can safely
        # distinguish its own incomplete work from pre-existing operator state.
        backend.mark_manual_hold_owned()
    if not backend.manual_hold_active():
        backend.metadata_gate("hold manual")
    if not backend.manual_hold_active():
        raise DeployHoldError("metadata gate did not establish the manual hold")


def _ensure_owned_control_mask(
    backend: DeployHoldBackend,
    timer: str,
) -> None:
    if not backend.link_is_owned(timer):
        target = backend.control_link_target(timer)
        if target is not None:
            raise DeployHoldError(
                f"unowned control path appeared during acquisition for "
                f"{timer}: {target!r}"
            )
        # As with the manual hold, the root-only receipt records intent first.
        # A retry may create a missing intended link, but never adopts one that
        # appeared without its ownership marker.
        backend.mark_link_owned(timer)
    target = backend.control_link_target(timer)
    if target is None:
        backend.create_control_mask(timer)
        target = backend.control_link_target(timer)
    if target != "/dev/null":
        raise DeployHoldError(
            f"owned control link changed for {timer}: {target!r}"
        )


def acquire_hold(backend: DeployHoldBackend) -> None:
    """Create or resume an authoritative strict hold acquisition."""
    backend.ensure_control_dir()
    if backend.receipt_exists():
        _require_phase(backend, PHASE_ACQUIRING)
    else:
        if backend.manual_hold_active():
            raise DeployHoldError("unowned manual hold already exists")
        for timer in TIMER_UNITS:
            target = backend.control_link_target(timer)
            if target is not None:
                raise DeployHoldError(
                    f"unowned control path already exists for {timer}: {target!r}"
                )
        backend.create_receipt()

    for timer in TIMER_UNITS:
        _ensure_owned_control_mask(backend, timer)
    # Make the authoritative trigger barrier effective before adding the
    # metadata gate. Any start already queued before this boundary is handled
    # by the exact service drain below.
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    backend.stop_units(TIMER_UNITS)
    _ensure_owned_manual_hold(backend)
    _drain_owned_services(backend)
    backend.write_phase(PHASE_HELD)


def verify_held(backend: DeployHoldBackend) -> None:
    """Re-prove the same receipt-owned hold after a NixOS switch."""
    _require_phase(backend, PHASE_HELD)
    _verify_authoritative_hold(backend)
    backend.write_phase(PHASE_HELD)


def recover_held(backend: DeployHoldBackend) -> None:
    """Return any receipt-owned incomplete phase to a strict held boundary."""
    backend.ensure_control_dir()
    if not backend.receipt_exists():
        raise DeployHoldError("deploy hold receipt is missing")
    phase = backend.read_phase()
    known_phases = {
        PHASE_ACQUIRING,
        PHASE_HELD,
        PHASE_PREPARED_CONTROLLED,
        PHASE_MAIN_TIMER_OPEN,
        PHASE_COMPLETE_PENDING,
    }
    if phase not in known_phases:
        raise DeployHoldError(f"cannot recover unknown phase: {phase!r}")
    for timer in TIMER_UNITS:
        _ensure_owned_control_mask(backend, timer)
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    backend.stop_units(TIMER_UNITS)
    _ensure_owned_manual_hold(backend)
    _drain_owned_services(backend)
    backend.clear_ordinary_invocation()
    backend.write_phase(PHASE_HELD)


def prepare_controlled(backend: DeployHoldBackend) -> None:
    """Retain every timer mask while starting one controlled main cycle."""
    _require_phase(backend, PHASE_HELD)
    _verify_authoritative_hold(backend)
    backend.metadata_gate("release manual")
    if backend.manual_hold_active():
        raise DeployHoldError("metadata gate did not release the owned manual hold")
    backend.unmark_manual_hold_owned()
    backend.start_unit(MAIN_SERVICE)
    backend.write_phase(PHASE_PREPARED_CONTROLLED)


def _release_owned_link(backend: DeployHoldBackend, timer: str) -> None:
    if not backend.link_is_owned(timer):
        raise DeployHoldError(f"refusing to remove unowned control link: {timer}")
    target = backend.control_link_target(timer)
    if target != "/dev/null":
        raise DeployHoldError(f"owned control link changed for {timer}: {target!r}")
    backend.remove_control_mask(timer)
    backend.unmark_link_owned(timer)


def open_main_timer(backend: DeployHoldBackend) -> None:
    """Open only the main timer after PR1 verifies the controlled cycle."""
    _require_phase(backend, PHASE_PREPARED_CONTROLLED)
    if backend.manual_hold_is_owned() or backend.manual_hold_active():
        raise DeployHoldError("manual hold still exists before main-timer release")
    _assert_owned_links(backend, TIMER_UNITS)
    _drain_owned_services(backend)
    _release_owned_link(backend, MAIN_TIMER)
    backend.daemon_reload()
    _assert_load_states(
        backend,
        masked=(UNFINDABLE_TIMER, WATCHDOG_TIMER),
        loaded=(MAIN_TIMER,),
    )
    backend.start_unit(MAIN_TIMER)
    state = backend.unit_state(MAIN_TIMER)
    if state.active_state != "active":
        raise DeployHoldError(
            f"main timer did not start: ActiveState={state.active_state}"
        )
    backend.write_phase(PHASE_MAIN_TIMER_OPEN)


def finish_release(
    backend: DeployHoldBackend,
    ordinary_invocation: str,
) -> None:
    """Open remaining timers after PR1 captures the ordinary successor."""
    _require_phase(backend, PHASE_MAIN_TIMER_OPEN)
    _validate_invocation_id(ordinary_invocation)
    _assert_owned_links(backend, (UNFINDABLE_TIMER, WATCHDOG_TIMER))
    if backend.control_link_target(MAIN_TIMER) is not None:
        raise DeployHoldError("main timer control path reappeared before release")
    backend.write_ordinary_invocation(ordinary_invocation)
    for timer in (UNFINDABLE_TIMER, WATCHDOG_TIMER):
        _release_owned_link(backend, timer)
    backend.daemon_reload()
    _assert_load_states(backend, masked=(), loaded=TIMER_UNITS)
    for timer in (UNFINDABLE_TIMER, WATCHDOG_TIMER):
        backend.start_unit(timer)
    backend.metadata_gate("resume-if-clear")
    for timer in TIMER_UNITS:
        state = backend.unit_state(timer)
        if state.active_state != "active":
            raise DeployHoldError(
                f"released timer is not active for {timer}: {state.active_state}"
            )
    backend.write_phase(PHASE_COMPLETE_PENDING)


def complete_release(
    backend: DeployHoldBackend,
    verified_invocation: str,
) -> None:
    """Clear the receipt after PR1 verifies the captured ordinary successor."""
    if not backend.receipt_exists() and backend.retired_receipt_exists():
        backend.finish_retired_receipt()
        return
    _require_phase(backend, PHASE_COMPLETE_PENDING)
    _validate_invocation_id(verified_invocation)
    captured = backend.read_ordinary_invocation()
    if captured != verified_invocation:
        raise DeployHoldError(
            f"verified invocation does not match captured successor: "
            f"{verified_invocation!r} != {captured!r}"
        )
    if backend.manual_hold_is_owned() or backend.manual_hold_active():
        raise DeployHoldError("manual hold remains at release completion")
    if backend.owned_link_units():
        raise DeployHoldError("owned timer links remain at release completion")
    for timer in TIMER_UNITS:
        target = backend.control_link_target(timer)
        if target is not None:
            raise DeployHoldError(
                f"timer control path exists at release completion for {timer}: "
                f"{target!r}"
            )
        state = backend.unit_state(timer)
        if state.load_state != "loaded" or state.active_state != "active":
            raise DeployHoldError(
                f"timer is not restored at release completion for {timer}: {state}"
            )
    backend.remove_receipt()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage Cratedigger's authoritative deployment hold",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in (
        "acquire",
        "verify-held",
        "recover-held",
        "prepare-controlled",
        "open-main-timer",
    ):
        subparsers.add_parser(command)
    finish = subparsers.add_parser("finish-release")
    finish.add_argument("ordinary_invocation")
    complete = subparsers.add_parser("complete")
    complete.add_argument("verified_invocation")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if os.geteuid() != 0:
        print("cratedigger-deploy-hold must run as root on doc2", file=sys.stderr)
        return 1
    backend = RealSystemdBackend()
    try:
        if args.command == "acquire":
            acquire_hold(backend)
        elif args.command == "verify-held":
            verify_held(backend)
        elif args.command == "recover-held":
            recover_held(backend)
        elif args.command == "prepare-controlled":
            prepare_controlled(backend)
        elif args.command == "open-main-timer":
            open_main_timer(backend)
        elif args.command == "finish-release":
            finish_release(backend, args.ordinary_invocation)
        elif args.command == "complete":
            complete_release(backend, args.verified_invocation)
        else:
            raise DeployHoldError(f"unknown command: {args.command}")
    except (DeployHoldError, OSError, subprocess.SubprocessError) as exc:
        print(f"cratedigger-deploy-hold: {exc}", file=sys.stderr)
        return 1
    print(args.command)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
