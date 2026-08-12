#!/usr/bin/env python3
"""Acquire and release Cratedigger's strict systemd deployment hold.

The fixed unit set and root-owned runtime receipt are deliberate. This helper
never accepts arbitrary unit names, never masks a service, and never removes a
control link it did not create and record itself.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeGuard

CONTROL_DIR = "/run/systemd/system.control"
STATE_DIR = Path("/run/cratedigger-deploy-hold")
STATE_STAGING_DIR = Path("/run/.cratedigger-deploy-hold.creating")
STATE_RETIRED_DIR = Path("/run/.cratedigger-deploy-hold.retired")
METADATA_GATE_STATE_DIR = Path("/var/lib/cratedigger-metadata-gate")
METADATA_GATE_HOLD_DIR = METADATA_GATE_STATE_DIR / "holds"
METADATA_MANUAL_HOLD = METADATA_GATE_HOLD_DIR / "manual"

MAIN_TIMER = "cratedigger.timer"
UNFINDABLE_TIMER = "cratedigger-unfindable.timer"
WATCHDOG_TIMER = "cratedigger-metadata-gate-watchdog.timer"
TIMER_UNITS = (MAIN_TIMER, UNFINDABLE_TIMER, WATCHDOG_TIMER)

MAIN_SERVICE = "cratedigger.service"
UNFINDABLE_SERVICE = "cratedigger-unfindable.service"
WATCHDOG_SERVICE = "cratedigger-metadata-gate-watchdog.service"
WEB_SERVICE = "cratedigger-web.service"
IMPORTER_SERVICE = "cratedigger-importer.service"
PREVIEW_SERVICE = "cratedigger-import-preview-worker.service"
YOUTUBE_SERVICE = "cratedigger-youtube-ingest.service"
CONTROLLED_WORKER_UNITS = (WEB_SERVICE, PREVIEW_SERVICE, IMPORTER_SERVICE)
PRODUCER_SERVICE_UNITS = (
    MAIN_SERVICE,
    UNFINDABLE_SERVICE,
    WATCHDOG_SERVICE,
    YOUTUBE_SERVICE,
)
SERVICE_UNITS = (*PRODUCER_SERVICE_UNITS, *CONTROLLED_WORKER_UNITS)
# Timer-driven producers that stop themselves once their own timer is
# masked: each is a Type=oneshot triggered only by that timer, so once the
# timer is stopped the current invocation (if any) finishes naturally and
# the unit goes idle -- nothing needs to actively stop it. This is the
# grouping #1078's acquire-side producer drain (before the gate hold) uses.
# The watchdog unit (cratedigger-metadata-gate-watchdog.{service,timer}) is
# nixosconfig-owned, not defined in this repo's nix/module.nix, so its shape
# is not something this module can cite from here; what this module DOES
# verify is that it is absent from the metadata gate's own guarded_units
# (verify_controlled_start_contract's expected_guarded, below), which is why
# this pre-hold drain -- not a gate-hold-side stop -- is the only place it is
# ever handled. YouTube ingest is deliberately excluded from this grouping:
# it is Type=simple, wantedBy=multi-user.target, Restart=on-failure, no timer
# at all -- an always-on daemon nothing before the gate hold ever asks to
# stop, so draining it there waits the full service-drain timeout for
# nothing (#1078 MUST FIX 1, nix/module.nix cratedigger-youtube-ingest).
TIMER_DRIVEN_PRODUCER_UNITS = (MAIN_SERVICE, UNFINDABLE_SERVICE, WATCHDOG_SERVICE)
# The units abort_hold explicitly starts and proves stably active when it
# releases the receipt-owned manual gate hold -- not "everything the gate
# hold actually stops": cratedigger.timer and cratedigger.service are also
# gate-guarded (both are in GATE_GUARDED_UNITS/GATE_RESUME_UNITS below,
# alongside these four), but abort never restarts either here directly. The
# service comes back either through the timer once its control-link mask is
# released later in the same function, or directly via that same
# resume-if-clear call's resume_units; the timer itself comes back only
# through its own control-link release.
GATE_STOPPED_UNITS = (YOUTUBE_SERVICE, *CONTROLLED_WORKER_UNITS)
# The metadata gate's own complete guarded/resume unit sets --
# verify_controlled_start_contract's expected_guarded/expected_resume, below,
# are built from these two tuples word-for-word, so they are the single
# source of truth for "what the gate actually guards": cratedigger.timer and
# cratedigger.service, plus the three controlled workers and YouTube ingest.
# GATE_STOPPED_UNITS above is a narrower, different-purpose set (what
# abort_hold explicitly restarts) and must never be used to derive "what the
# gate guards" -- that conflation was issue #1100 item 1.
GATE_GUARDED_UNITS = (
    MAIN_TIMER,
    MAIN_SERVICE,
    WEB_SERVICE,
    IMPORTER_SERVICE,
    PREVIEW_SERVICE,
    YOUTUBE_SERVICE,
)
GATE_RESUME_UNITS = (
    MAIN_SERVICE,
    MAIN_TIMER,
    WEB_SERVICE,
    IMPORTER_SERVICE,
    PREVIEW_SERVICE,
    YOUTUBE_SERVICE,
)

MAIN_START_INHIBITOR = METADATA_GATE_STATE_DIR / f"inhibit-{MAIN_SERVICE}"
YOUTUBE_START_INHIBITOR = METADATA_GATE_STATE_DIR / f"inhibit-{YOUTUBE_SERVICE}"
START_INHIBITORS = {
    MAIN_SERVICE: MAIN_START_INHIBITOR,
    YOUTUBE_SERVICE: YOUTUBE_START_INHIBITOR,
}

PHASE_ACQUIRING = "acquiring"
PHASE_HELD = "held"
PHASE_PREPARED_CONTROLLED = "prepared-controlled"
PHASE_MAIN_TIMER_OPEN = "main-timer-open"
PHASE_COMPLETE_PENDING = "complete-pending"

# Real deployments run this tool as root; every receipt, marker, and
# metadata-gate artifact it owns or validates must be root-owned, so every
# ownership check in RealSystemdBackend compares against this constant
# rather than a bare literal ``0``. It is never a security policy switch --
# production never sets it to anything else -- but tests can patch it to
# the current process's uid (`unittest.mock.patch` on this module
# attribute) to exercise the SAME real filesystem validation logic against
# a real tmpdir without requiring the test runner itself to run as root.
_ROOT_UID = 0

_RECEIPT_VERSION = "cratedigger-deploy-hold-v1"
_RECEIPT_FILE = "receipt"
_PHASE_FILE = "phase"
_MANUAL_MARKER = "owned-manual-hold"
_LINK_MARKER_PREFIX = "owned-link-"
_INHIBITOR_MARKER_PREFIX = "owned-inhibitor-"
_INVOCATION_FILE = "ordinary-invocation"
# Persistent siblings of the two owned-object classes that outlive a reboot
# (#1096): both live in METADATA_GATE_STATE_DIR, beside the manual hold file
# and each inhibitor file they mark. Consulted only when no receipt exists --
# while a receipt is present, the tmpfs markers above remain sole authority.
_PERSISTENT_MANUAL_MARKER = "deploy-hold-owned-manual"
_PERSISTENT_INHIBITOR_MARKER_PREFIX = "deploy-hold-owned-inhibit-"
_INVOCATION_RE = re.compile(r"[0-9a-f]{32}")
# Bounds every ``_drain_services`` call whose unit set includes
# ``cratedigger-unfindable.service`` (line numbers re-verified against the
# tree merged with #1096's correction round): ``_verify_authoritative_
# hold`` (:1413), ``_drain_producers_then_hold`` (:1640, :1646),
# ``recover_held``'s cold-start branch (:1741), and ``open_main_timer``
# (:2236) -- verified individually against ``SERVICE_UNITS`` /
# ``TIMER_DRIVEN_PRODUCER_UNITS`` / ``PRODUCER_SERVICE_UNITS``, which all
# include ``UNFINDABLE_SERVICE``. Must exceed
# the longest BOUNDED run among the units it drains, or an
# acquire/verify/recover launched while that run is mid-flight times out
# here with a misleading ``DeployHoldError`` instead of just waiting for
# it to finish. That is deliberately NOT "the longest of these units'
# TimeoutStartSec" (round 1's wrong framing, R1): the watchdog service
# (``cratedigger-metadata-gate-watchdog.service``) has
# ``TimeoutStartUSec=infinity`` -- no start timeout at all -- and is
# nixosconfig-owned, so its shape cannot be cited from this repo (see the
# ``TIMER_DRIVEN_PRODUCER_UNITS`` comment above); ``cratedigger.service``
# caps at 1h. ``cratedigger-unfindable.service`` at ``TimeoutStartSec=
# "5h"`` (nix/module.nix, issue #1112 item 1) is the only member with a
# materially long bounded run, so it is what this constant is sized
# against. 21600.0 (6h) keeps a 1h margin over it. Resize alongside any
# future change to that unit's ``TimeoutStartSec``.
_PRODUCER_DRAIN_TIMEOUT_SECONDS = 21600.0
# Bounds ``_wait_controlled_workers_active`` (:1496 -- five call sites,
# line numbers re-verified against the tree merged with #1096's
# correction round: :1956 from ``_adopt_persistent_markers_or_refuse``,
# :2123/:2157 from ``abort_hold``, :2201/:2210 from
# ``prepare_controlled``) and the main+YouTube drain at :2212
# (``prepare_controlled``, ``_drain_services(backend, (MAIN_SERVICE,
# YOUTUBE_SERVICE))``). No unit set any of these five calls ever bounds
# contains ``cratedigger-unfindable.service``, so it keeps the pre-#1112
# value (2h) rather than following ``_PRODUCER_DRAIN_TIMEOUT_SECONDS`` up
# to 6h (round 2, R2).
#
# Counted honestly after #1096's correction round added the fifth call
# site (:1956): within a SINGLE invocation of ``abort_hold`` itself, the
# worst case is still exactly two sequential waits (:2123 then :2157,
# both on the receipt-owned path) -- unchanged from before #1096, and
# ``_adopt_persistent_markers_or_refuse``'s receiptless path (:1956) is
# mutually exclusive with that path, never both in the same call. But
# #1096 introduced a genuinely NEW troubleshooting sequence this budget
# did not previously have to bound: the retired-receipt-plus-unrelated-
# orphan-marker "two-run" case (correction round, N6) can see abort's
# receipt-owned path (2 waits) on one invocation and adoption's
# receiptless path (1 wait) on the very next, for up to three sequential
# 2h waits across that realistic pair of calls where at most two existed
# before #1096 existed at all. Silently tripling this budget to 6h would
# have multiplied that same worst case, an unrelated and unintended
# regression.
_DRAIN_TIMEOUT_SECONDS = 7200.0
_POLL_SECONDS = 1.0
_STABLE_SAMPLES = 2
# The automation queue drains while the importer/preview controlled workers
# are still running (#1078) -- a much shorter horizon than a full service
# drain. 30 minutes matches the bound the deploy skill already uses for the
# analogous "wait for the triggered nixos-upgrade invocation" step, and a
# genuinely stuck queue should surface quickly rather than hold the deploy
# for the full multi-hour service-drain budget. Polling is a live
# pipeline-cli subprocess round trip, so it uses its own, coarser cadence.
_QUEUE_DRAIN_TIMEOUT_SECONDS = 1800.0
_QUEUE_POLL_SECONDS = 5.0


class DeployHoldError(RuntimeError):
    """The strict hold could not prove the requested lifecycle boundary."""


def _is_json_object(value: object) -> TypeGuard[dict[str, object]]:
    """Narrow decoded JSON without adding non-stdlib deploy dependencies."""
    return isinstance(value, dict)


def _units_line(prefix: str, units: tuple[str, ...]) -> str:
    """Build the metadata gate's exact ``<prefix>=(...)`` shell literal."""
    return f"{prefix}=(" + " ".join(units) + ")"


# Composed once at import time -- not inside verify_controlled_start_contract
# -- so these are the literal module-level names that method consumes, and a
# future mis-wire (e.g. building the guarded line from GATE_RESUME_UNITS) is
# a fact a plain byte-identity pin can catch directly, rather than a
# call-site composition only observable by actually running
# verify_controlled_start_contract (the VM check or a live acquire).
GATE_GUARDED_LINE = _units_line("guarded_units", GATE_GUARDED_UNITS)
GATE_RESUME_LINE = _units_line("resume_units", GATE_RESUME_UNITS)


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


@dataclass(frozen=True)
class LifecyclePreflight:
    active_automation_jobs: int
    recovery_required_jobs: int
    dirty_downloading_rows: int
    malformed_enqueued_at_rows: int

    def dirty_fields(self) -> dict[str, int]:
        return {
            name: value
            for name, value in (
                ("active_automation_jobs", self.active_automation_jobs),
                ("recovery_required_jobs", self.recovery_required_jobs),
                ("dirty_downloading_rows", self.dirty_downloading_rows),
                ("malformed_enqueued_at_rows", self.malformed_enqueued_at_rows),
            )
            if value != 0
        }


class DeployHoldBackend(Protocol):
    def verify_controlled_start_contract(self) -> None: ...
    def lifecycle_preflight(self) -> LifecyclePreflight: ...
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
    def persistent_manual_marker_exists(self) -> bool: ...
    def write_persistent_manual_marker(self) -> None: ...
    def remove_persistent_manual_marker(self) -> None: ...
    def mark_link_owned(self, timer: str) -> None: ...
    def unmark_link_owned(self, timer: str) -> None: ...
    def link_is_owned(self, timer: str) -> bool: ...
    def owned_link_units(self) -> tuple[str, ...]: ...
    def mark_inhibitor_owned(self, service: str) -> None: ...
    def unmark_inhibitor_owned(self, service: str) -> None: ...
    def inhibitor_is_owned(self, service: str) -> bool: ...
    def owned_inhibitor_units(self) -> tuple[str, ...]: ...
    def persistent_inhibitor_marker_exists(self, service: str) -> bool: ...
    def write_persistent_inhibitor_marker(self, service: str) -> None: ...
    def remove_persistent_inhibitor_marker(self, service: str) -> None: ...
    def inhibitor_exists(self, service: str) -> bool: ...
    def create_start_inhibitor(self, service: str) -> None: ...
    def remove_start_inhibitor(self, service: str) -> None: ...
    def write_ordinary_invocation(self, invocation_id: str) -> None: ...
    def read_ordinary_invocation(self) -> str: ...
    def clear_ordinary_invocation(self) -> None: ...
    def manual_hold_active(self) -> bool: ...
    def metadata_gate(self, command: str) -> int: ...
    def metadata_hold_reasons(self) -> tuple[str, ...]: ...
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

    @staticmethod
    def _unit_file_text(unit: str) -> str:
        RealSystemdBackend._validate_unit(unit, SERVICE_UNITS)
        return subprocess.run(
            ("systemctl", "cat", unit),
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def verify_controlled_start_contract(self) -> None:
        """Verify the independently deployed producer-inhibitor prerequisite."""
        expected_conditions = {
            MAIN_SERVICE: f"ConditionPathExists=!{MAIN_START_INHIBITOR}",
            YOUTUBE_SERVICE: (
                f"ConditionPathExists=!{YOUTUBE_START_INHIBITOR}"
            ),
        }
        gate_paths: set[str] = set()
        for service, condition in expected_conditions.items():
            source = self._unit_file_text(service)
            if source.splitlines().count(condition) != 1:
                raise DeployHoldError(
                    f"controlled-start prerequisite changed for {service}"
                )
            execution = self._run((
                "systemctl",
                "show",
                service,
                "--property=ExecCondition",
                "--value",
            )).stdout
            match = re.search(r"\bpath=([^ ;]+)", execution)
            if match is None:
                raise DeployHoldError(
                    f"metadata-gate ExecCondition is missing for {service}"
                )
            gate_paths.add(match.group(1))

        if len(gate_paths) != 1:
            raise DeployHoldError(
                "controlled producers do not share one metadata-gate prerequisite"
            )
        for service in CONTROLLED_WORKER_UNITS:
            source = self._unit_file_text(service)
            if any(
                str(inhibitor) in source
                for inhibitor in START_INHIBITORS.values()
            ):
                raise DeployHoldError(
                    f"controlled worker unexpectedly uses a producer inhibitor: "
                    f"{service}"
                )

        gate_source = Path(gate_paths.pop()).read_text(encoding="utf-8")
        if (
            gate_source.splitlines().count(GATE_GUARDED_LINE) != 1
            or gate_source.splitlines().count(GATE_RESUME_LINE) != 1
        ):
            raise DeployHoldError(
                "metadata-gate guarded/resume unit contract is not the "
                "verified controlled-start prerequisite"
            )

    @staticmethod
    def _single_json_cell(output: str, expected_header: str) -> dict[str, object]:
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if (
            len(lines) != 4
            or lines[0] != expected_header
            or lines[3] != "(1 row)"
        ):
            raise DeployHoldError(
                f"unexpected pipeline-cli preflight output: {output!r}"
            )
        try:
            value = json.loads(lines[2])
        except json.JSONDecodeError as exc:
            raise DeployHoldError(
                "pipeline-cli preflight did not return JSON"
            ) from exc
        if not _is_json_object(value):
            raise DeployHoldError("pipeline-cli preflight JSON is not an object")
        return value

    def _pipeline_query(self, sql: str) -> str:
        password_file = Path("/run/secrets/cratedigger-pgpass")
        password_lines = password_file.read_text(encoding="utf-8").splitlines()
        passwords = [
            line.partition("=")[2]
            for line in password_lines
            if line.startswith("PGPASSWORD=")
        ]
        if len(passwords) != 1 or not passwords[0]:
            raise DeployHoldError(
                "cratedigger-pgpass does not contain one PGPASSWORD value"
            )
        environment = os.environ.copy()
        environment["PGPASSWORD"] = passwords[0]
        proc = subprocess.run(
            ("pipeline-cli", "query", "-"),
            input=sql,
            check=True,
            capture_output=True,
            text=True,
            env=environment,
        )
        return proc.stdout

    def lifecycle_preflight(self) -> LifecyclePreflight:
        """Query the live old-lifecycle schema, then prove its boundary clean."""
        schema = self._single_json_cell(
            self._pipeline_query(
                """
                SELECT json_build_object(
                    'album_requests',
                    COALESCE(
                        (
                            SELECT json_agg(column_name ORDER BY column_name)
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = 'album_requests'
                              AND column_name IN (
                                  'id', 'status', 'active_download_state'
                              )
                        ),
                        '[]'::json
                    ),
                    'import_jobs',
                    COALESCE(
                        (
                            SELECT json_agg(column_name ORDER BY column_name)
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = 'import_jobs'
                              AND column_name IN (
                                  'id', 'job_type', 'status'
                              )
                        ),
                        '[]'::json
                    )
                ) AS schema_contract
                """
            ),
            "schema_contract",
        )
        expected_schema = {
            "album_requests": ["active_download_state", "id", "status"],
            "import_jobs": ["id", "job_type", "status"],
        }
        if schema != expected_schema:
            raise DeployHoldError(
                f"old-lifecycle preflight schema changed: {schema!r}"
            )

        counts = self._single_json_cell(
            self._pipeline_query(
                """
                SELECT json_build_object(
                    'active_automation_jobs',
                    (
                        SELECT count(*)
                        FROM import_jobs
                        WHERE job_type = 'automation_import'
                          AND status IN (
                              'queued', 'running', 'recovery_required'
                          )
                    ),
                    'recovery_required_jobs',
                    (
                        SELECT count(*)
                        FROM import_jobs
                        WHERE status = 'recovery_required'
                    ),
                    'dirty_downloading_rows',
                    (
                        SELECT count(*)
                        FROM album_requests
                        WHERE status = 'downloading'
                          AND (
                              active_download_state
                                  ? 'processing_started_at'
                              OR NULLIF(
                                  active_download_state->>'current_path',
                                  ''
                              ) IS NOT NULL
                              OR active_download_state
                                  ? 'import_subprocess_started_at'
                          )
                    ),
                    'malformed_enqueued_at_rows',
                    (
                        SELECT count(*)
                        FROM album_requests
                        WHERE status = 'downloading'
                          AND NOT COALESCE((
                              active_download_state ? 'enqueued_at'
                              AND jsonb_typeof(
                                  active_download_state->'enqueued_at'
                              ) = 'string'
                              AND NULLIF(
                                  active_download_state->>'enqueued_at',
                                  ''
                              ) IS NOT NULL
                              AND pg_input_is_valid(
                                  active_download_state->>'enqueued_at',
                                  'timestamp with time zone'
                              )
                          ), FALSE)
                    )
                ) AS lifecycle_preflight
                """
            ),
            "lifecycle_preflight",
        )
        expected_keys = {
            "active_automation_jobs",
            "recovery_required_jobs",
            "dirty_downloading_rows",
            "malformed_enqueued_at_rows",
        }
        if set(counts) != expected_keys:
            raise DeployHoldError(
                f"invalid lifecycle preflight counts: {counts!r}"
            )

        def count(name: str) -> int:
            value = counts[name]
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
            ):
                raise DeployHoldError(
                    f"invalid lifecycle preflight counts: {counts!r}"
                )
            return value

        return LifecyclePreflight(
            active_automation_jobs=count("active_automation_jobs"),
            recovery_required_jobs=count("recovery_required_jobs"),
            dirty_downloading_rows=count("dirty_downloading_rows"),
            malformed_enqueued_at_rows=count("malformed_enqueued_at_rows"),
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
            or info.st_uid != _ROOT_UID
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
        if info.st_uid != _ROOT_UID or stat.S_IMODE(info.st_mode) != 0o700:
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
                or info.st_uid != _ROOT_UID
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
        if not stat.S_ISREG(info.st_mode) or info.st_uid != _ROOT_UID:
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
                or info.st_uid != _ROOT_UID
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

    @staticmethod
    def _persistent_marker_path(name: str) -> Path:
        if not name or "/" in name or name in {".", ".."}:
            raise DeployHoldError(f"invalid persistent ownership marker: {name!r}")
        return METADATA_GATE_STATE_DIR / name

    @classmethod
    def _persistent_marker_exists(cls, name: str) -> bool:
        cls._validate_metadata_gate_state_dir()
        path = cls._persistent_marker_path(name)
        if not os.path.lexists(path):
            return False
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
            or stat.S_IMODE(info.st_mode) != 0o600
        ):
            raise DeployHoldError(
                f"persistent ownership marker has unsafe state: {name}"
            )
        return True

    @classmethod
    def _write_persistent_marker(cls, name: str, value: str) -> None:
        """Idempotently ensure the persistent sibling marker exists.

        Every caller writes this before the persistent object it names is
        ever created (#1096) -- the same intent-before-mutation discipline
        the tmpfs markers already follow, so an object without its marker is
        provably foreign. A retry after a crash between this write and the
        object's own creation must not fail here: the marker may already
        exist from the interrupted attempt, so this tolerates (and
        validates) an existing marker rather than requiring absence.
        """
        cls._validate_metadata_gate_state_dir()
        path = cls._persistent_marker_path(name)
        if os.path.lexists(path):
            # _persistent_marker_exists re-checks lexists() itself; barring
            # a concurrent unlink racing this exact instant (TOCTOU, not a
            # shape this single-threaded tool defends against elsewhere
            # either), it can only return True here or raise its own
            # DeployHoldError -- never False. Its validation is what we
            # want; there is no separate "unsafe state" outcome to invent.
            cls._persistent_marker_exists(name)
            return
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(value + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    @classmethod
    def _remove_persistent_marker(cls, name: str) -> None:
        """Idempotently clear the persistent sibling marker.

        Every caller removes this only after the persistent object it names
        is already gone, so a retry that finds the marker already removed
        (a prior attempt got this far before failing) is not an error.
        """
        cls._validate_metadata_gate_state_dir()
        path = cls._persistent_marker_path(name)
        if not os.path.lexists(path):
            return
        info = path.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
        ):
            raise DeployHoldError(
                f"persistent ownership marker has unsafe state: {name}"
            )
        path.unlink()

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
        # Persistent marker first: it must exist on disk before the manual
        # hold object itself is ever created, so a reboot between the two
        # can never find the object without its sibling marker (#1096).
        self.write_persistent_manual_marker()
        self._write_marker(_MANUAL_MARKER, "manual", replace=False)

    def unmark_manual_hold_owned(self) -> None:
        if self._read_marker(_MANUAL_MARKER) != "manual":
            raise DeployHoldError("manual hold ownership marker changed")
        # Every caller reaches this only after the manual hold object is
        # already released; the persistent sibling clears first (tolerating
        # an already-removed marker on retry), then the tmpfs marker.
        self.remove_persistent_manual_marker()
        self._marker_path(_MANUAL_MARKER).unlink()

    def manual_hold_is_owned(self) -> bool:
        path = self._marker_path(_MANUAL_MARKER)
        if not path.exists():
            return False
        return self._read_marker(_MANUAL_MARKER) == "manual"

    def persistent_manual_marker_exists(self) -> bool:
        return self._persistent_marker_exists(_PERSISTENT_MANUAL_MARKER)

    def write_persistent_manual_marker(self) -> None:
        self._write_persistent_marker(_PERSISTENT_MANUAL_MARKER, "manual")

    def remove_persistent_manual_marker(self) -> None:
        self._remove_persistent_marker(_PERSISTENT_MANUAL_MARKER)

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

    @staticmethod
    def _validate_inhibited_service(service: str) -> Path:
        try:
            return START_INHIBITORS[service]
        except KeyError as exc:
            raise DeployHoldError(
                f"service outside fixed inhibitor scope: {service}"
            ) from exc

    @staticmethod
    def _inhibitor_marker(service: str) -> str:
        RealSystemdBackend._validate_inhibited_service(service)
        return _INHIBITOR_MARKER_PREFIX + service

    def mark_inhibitor_owned(self, service: str) -> None:
        # Same ordering discipline as mark_manual_hold_owned: the persistent
        # marker exists before _ensure_owned_start_inhibitor ever creates
        # the inhibitor file itself (#1096).
        self.write_persistent_inhibitor_marker(service)
        self._write_marker(
            self._inhibitor_marker(service),
            service,
            replace=False,
        )

    def unmark_inhibitor_owned(self, service: str) -> None:
        marker = self._inhibitor_marker(service)
        if self._read_marker(marker) != service:
            raise DeployHoldError(
                f"start-inhibitor ownership marker changed: {service}"
            )
        self.remove_persistent_inhibitor_marker(service)
        self._marker_path(marker).unlink()

    @staticmethod
    def _persistent_inhibitor_marker_name(service: str) -> str:
        RealSystemdBackend._validate_inhibited_service(service)
        return _PERSISTENT_INHIBITOR_MARKER_PREFIX + service

    def persistent_inhibitor_marker_exists(self, service: str) -> bool:
        return self._persistent_marker_exists(
            self._persistent_inhibitor_marker_name(service)
        )

    def write_persistent_inhibitor_marker(self, service: str) -> None:
        self._write_persistent_marker(
            self._persistent_inhibitor_marker_name(service), service
        )

    def remove_persistent_inhibitor_marker(self, service: str) -> None:
        self._remove_persistent_marker(
            self._persistent_inhibitor_marker_name(service)
        )

    def inhibitor_is_owned(self, service: str) -> bool:
        marker_name = self._inhibitor_marker(service)
        marker = self._marker_path(marker_name)
        if not marker.exists():
            return False
        return self._read_marker(marker_name) == service

    def owned_inhibitor_units(self) -> tuple[str, ...]:
        self._validate_receipt()
        owned: list[str] = []
        for entry in STATE_DIR.iterdir():
            if not entry.name.startswith(_INHIBITOR_MARKER_PREFIX):
                continue
            service = entry.name.removeprefix(_INHIBITOR_MARKER_PREFIX)
            self._validate_inhibited_service(service)
            if self._read_marker(entry.name) != service:
                raise DeployHoldError(
                    f"start-inhibitor ownership marker changed: {service}"
                )
            owned.append(service)
        return tuple(sorted(owned))

    @staticmethod
    def _validate_inhibitor_file(path: Path, service: str) -> None:
        try:
            info = path.lstat()
        except FileNotFoundError as exc:
            raise DeployHoldError(
                f"owned start inhibitor is missing: {service}"
            ) from exc
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
            or stat.S_IMODE(info.st_mode) != 0o600
            or path.read_text(encoding="utf-8") != _RECEIPT_VERSION + "\n"
        ):
            raise DeployHoldError(
                f"owned start inhibitor changed for {service}"
            )

    @staticmethod
    def _validate_metadata_gate_state_dir() -> None:
        try:
            info = METADATA_GATE_STATE_DIR.lstat()
        except FileNotFoundError as exc:
            raise DeployHoldError(
                "metadata-gate state directory is missing"
            ) from exc
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
            or stat.S_IMODE(info.st_mode) not in {0o700, 0o755}
        ):
            raise DeployHoldError(
                "metadata-gate state directory is not root-owned mode 0700/0755"
            )

    def inhibitor_exists(self, service: str) -> bool:
        self._validate_metadata_gate_state_dir()
        path = self._validate_inhibited_service(service)
        return os.path.lexists(path)

    def create_start_inhibitor(self, service: str) -> None:
        self._validate_metadata_gate_state_dir()
        path = self._validate_inhibited_service(service)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(_RECEIPT_VERSION + "\n")
            handle.flush()
            os.fsync(handle.fileno())

    def remove_start_inhibitor(self, service: str) -> None:
        self._validate_metadata_gate_state_dir()
        path = self._validate_inhibited_service(service)
        self._validate_inhibitor_file(path, service)
        path.unlink()

    def write_ordinary_invocation(self, invocation_id: str) -> None:
        self._write_marker(_INVOCATION_FILE, invocation_id, replace=False)

    def read_ordinary_invocation(self) -> str:
        return self._read_marker(_INVOCATION_FILE)

    def clear_ordinary_invocation(self) -> None:
        path = self._marker_path(_INVOCATION_FILE)
        if os.path.lexists(path):
            self._remove_marker(_INVOCATION_FILE)

    def manual_hold_active(self) -> bool:
        self._validate_metadata_gate_state_dir()
        if not os.path.lexists(METADATA_MANUAL_HOLD):
            return False
        info = METADATA_MANUAL_HOLD.lstat()
        if (
            not stat.S_ISREG(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
        ):
            raise DeployHoldError("manual metadata hold has unsafe state")
        return True

    def metadata_gate(self, command: str) -> int:
        commands = {
            "hold manual": ("hold", "manual"),
            "release manual": ("release", "manual"),
            "resume-if-clear": ("resume-if-clear",),
        }
        args = commands.get(command)
        if args is None:
            raise DeployHoldError(f"metadata-gate command outside fixed scope: {command}")
        proc = self._run(
            ("cratedigger-metadata-gate", *args),
            check=command != "resume-if-clear",
        )
        return proc.returncode

    def metadata_hold_reasons(self) -> tuple[str, ...]:
        self._validate_metadata_gate_state_dir()
        try:
            info = METADATA_GATE_HOLD_DIR.lstat()
        except FileNotFoundError as exc:
            raise DeployHoldError("metadata-gate holds directory is missing") from exc
        if (
            not stat.S_ISDIR(info.st_mode)
            or stat.S_ISLNK(info.st_mode)
            or info.st_uid != _ROOT_UID
        ):
            raise DeployHoldError("metadata-gate holds path is not root-owned")
        reasons: list[str] = []
        for entry in METADATA_GATE_HOLD_DIR.iterdir():
            entry_info = entry.lstat()
            if (
                not stat.S_ISREG(entry_info.st_mode)
                or stat.S_ISLNK(entry_info.st_mode)
                or entry_info.st_uid != _ROOT_UID
            ):
                raise DeployHoldError(
                    f"metadata-gate hold has unsafe state: {entry.name}"
                )
            reasons.append(entry.name)
        return tuple(sorted(reasons))

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
        self._validate_unit(unit, (*TIMER_UNITS, *SERVICE_UNITS))
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


def _drain_services(
    backend: DeployHoldBackend,
    services: tuple[str, ...],
    *,
    timeout_seconds: float,
) -> None:
    """Drain ``services`` to stably inactive and job-free.

    ``timeout_seconds`` is deliberately required, not defaulted: callers
    whose unit set includes ``cratedigger-unfindable.service`` pass
    ``_PRODUCER_DRAIN_TIMEOUT_SECONDS``; every other caller passes
    ``_DRAIN_TIMEOUT_SECONDS`` (review round 2, R2) -- see both
    constants' definitions for the exact call-site list each one bounds.
    """
    deadline = backend.monotonic() + timeout_seconds
    stable_samples = 0
    while backend.monotonic() < deadline:
        safe = True
        for service in services:
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


def _wait_automation_queue_drained(backend: DeployHoldBackend) -> None:
    """Wait for the drainable ``LifecyclePreflight`` fields to reach zero.

    ``active_automation_jobs`` and ``dirty_downloading_rows`` are drainable:
    at this point in acquisition the controlled workers (importer/preview)
    are still running -- the gate hold that would stop them has not been
    taken yet -- so they are what empties the queue and clears any
    mid-handoff row the drained main cycle left behind.

    The other two ``LifecyclePreflight`` fields, ``recovery_required_jobs``
    and ``malformed_enqueued_at_rows``, are anomalies nothing drains -- and
    ``recovery_required_jobs`` is itself counted inside
    ``active_automation_jobs``'s own SQL (``status IN ('queued', 'running',
    'recovery_required')``), so a stuck recovery-required job can make the
    drainable count above permanently unable to reach zero. Waiting out the
    full timeout for that would be both slow and misdiagnosed (reporting
    "queue" when the truth is a stuck anomaly), so this loop stops the
    moment either anomaly field is dirty and lets the still-to-be-taken gate
    hold plus ``_assert_clean_old_lifecycle`` report the complete, accurate
    field dict immediately afterward instead (#1078 MUST FIX 6).
    """
    deadline = backend.monotonic() + _QUEUE_DRAIN_TIMEOUT_SECONDS
    while True:
        preflight = backend.lifecycle_preflight()
        if preflight.active_automation_jobs == 0 and preflight.dirty_downloading_rows == 0:
            return
        if preflight.recovery_required_jobs != 0 or preflight.malformed_enqueued_at_rows != 0:
            return
        if backend.monotonic() >= deadline:
            raise DeployHoldError(
                "timed out waiting for the automation queue to drain: "
                f"active_automation_jobs={preflight.active_automation_jobs} "
                f"dirty_downloading_rows={preflight.dirty_downloading_rows}"
            )
        backend.sleep(_QUEUE_POLL_SECONDS)


def _verify_authoritative_hold(backend: DeployHoldBackend) -> None:
    if not backend.manual_hold_is_owned() or not backend.manual_hold_active():
        raise DeployHoldError("receipt-owned manual metadata hold is not active")
    _assert_owned_links(backend, TIMER_UNITS)
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    backend.stop_units(TIMER_UNITS)
    _drain_services(
        backend, SERVICE_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )
    _assert_no_start_inhibitors(backend)


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


def _assert_no_start_inhibitors(backend: DeployHoldBackend) -> None:
    if backend.owned_inhibitor_units():
        raise DeployHoldError("held phase retained owned producer inhibitors")
    for service in START_INHIBITORS:
        if backend.inhibitor_exists(service):
            raise DeployHoldError(
                f"unowned producer inhibitor exists for {service}"
            )


def _ensure_owned_start_inhibitor(
    backend: DeployHoldBackend,
    service: str,
) -> None:
    if not backend.inhibitor_is_owned(service):
        if backend.inhibitor_exists(service):
            raise DeployHoldError(
                f"unowned producer inhibitor appeared for {service}"
            )
        backend.mark_inhibitor_owned(service)
    if not backend.inhibitor_exists(service):
        backend.create_start_inhibitor(service)
    if not backend.inhibitor_exists(service):
        raise DeployHoldError(
            f"producer inhibitor was not established for {service}"
        )


def _release_owned_inhibitor(
    backend: DeployHoldBackend,
    service: str,
) -> None:
    if not backend.inhibitor_is_owned(service):
        raise DeployHoldError(
            f"refusing to remove unowned producer inhibitor: {service}"
        )
    if not backend.inhibitor_exists(service):
        raise DeployHoldError(
            f"owned producer inhibitor is missing: {service}"
        )
    backend.remove_start_inhibitor(service)
    backend.unmark_inhibitor_owned(service)


def _clear_owned_inhibitors(backend: DeployHoldBackend) -> None:
    owned = set(backend.owned_inhibitor_units())
    unexpected = owned - set(START_INHIBITORS)
    if unexpected:
        raise DeployHoldError(
            f"receipt owns unknown producer inhibitors: {sorted(unexpected)!r}"
        )
    for service in START_INHIBITORS:
        if service in owned:
            if backend.inhibitor_exists(service):
                _release_owned_inhibitor(backend, service)
            else:
                backend.unmark_inhibitor_owned(service)
        elif backend.inhibitor_exists(service):
            raise DeployHoldError(
                f"unowned producer inhibitor exists for {service}"
            )


def _wait_controlled_workers_active(
    backend: DeployHoldBackend,
    units: tuple[str, ...] = CONTROLLED_WORKER_UNITS,
) -> None:
    """Prove every one of ``units`` is stably active, not merely started.

    A ``systemctl start`` that a gate-guarded unit's ``ExecCondition``
    silently skips still returns success -- it is a condition skip, not a
    failure -- so a caller that only issues the start and never checks back
    cannot tell a real start from a silent no-op under a foreign gate hold
    (#1078 MUST FIX 2). Checking ``metadata_hold_reasons()`` first on every
    poll catches that: any hold present before or appearing during the wait,
    ours or a foreign one (e.g. the monthly discogs-import hold), fails this
    loudly instead of exiting 0 with the unit still down.
    """
    deadline = backend.monotonic() + _DRAIN_TIMEOUT_SECONDS
    stable_samples = 0
    while backend.monotonic() < deadline:
        if reasons := backend.metadata_hold_reasons():
            raise DeployHoldError(
                f"metadata gate became held while starting controlled workers: "
                f"{reasons!r}"
            )
        active = True
        for service in units:
            state = backend.unit_state(service)
            if (
                (state.active_state, state.sub_state) != ("active", "running")
                or backend.job_state(service) != JobState.none()
            ):
                active = False
        if active:
            stable_samples += 1
            if stable_samples >= _STABLE_SAMPLES:
                return
        else:
            stable_samples = 0
        backend.sleep(_POLL_SECONDS)
    raise DeployHoldError(
        f"timed out waiting for controlled workers to become stably active: "
        f"{units!r}"
    )


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


def _assert_clean_old_lifecycle(backend: DeployHoldBackend) -> None:
    """Prove the pre-migration lifecycle boundary the strict hold exists for."""
    preflight = backend.lifecycle_preflight()
    if dirty := preflight.dirty_fields():
        raise DeployHoldError(
            f"old lifecycle is not clean for migration: {dirty!r}"
        )


def _mask_and_stop_timers(backend: DeployHoldBackend) -> None:
    """Own every timer control mask, then stop the timers behind it.

    Shared by ``acquire_hold`` and ``recover_held``'s acquiring-phase branch:
    the authoritative trigger barrier must be effective -- masked, reloaded,
    and stopped -- before anything downstream drains or holds against it. Any
    start already queued before this boundary is handled by the exact service
    drains that follow.
    """
    for timer in TIMER_UNITS:
        _ensure_owned_control_mask(backend, timer)
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    backend.stop_units(TIMER_UNITS)


def _drain_producers_then_hold(backend: DeployHoldBackend) -> None:
    """Drain timer-driven producers and the automation queue, then hold.

    Order matters (#1078). The metadata gate's external tool stops every
    gate-guarded unit, including the importer and preview workers that are
    the only thing draining ``active_automation_jobs`` and
    ``dirty_downloading_rows``. Taking the hold first stops them before the
    queue ever gets a chance to drain -- an unrecoverable deadlock once
    combined with the old-lifecycle preflight this hold exists to prove
    clean, because nothing left running can ever satisfy it. Draining
    producers, waiting for the still-running importer/preview to empty the
    queue, and only then taking the hold removes that deadlock by
    construction; every later failure still leaves the deployment more
    quiesced than it started (timers stay held; the gate is taken exactly
    when this function returns successfully, never partially).

    Only ``TIMER_DRIVEN_PRODUCER_UNITS`` (main, unfindable, watchdog) drain
    here, pre-hold -- each stops itself once its timer is masked. YouTube
    ingest is deliberately NOT drained in this pre-hold phase: it is an
    always-on ``Type=simple`` daemon with no timer, so nothing before the
    gate hold ever asks it to stop, and waiting for it here would wait the
    full service-drain timeout for nothing (#1078 MUST FIX 1). No temporary
    start inhibitor is needed for this pre-hold window either: masking
    already blocks the timer trigger, and creating one would be a persistent
    ``/var/lib`` artifact that does not survive a host reboot alongside the
    ephemeral ``/run`` receipt that would own it (#1078 MUST FIX 5) -- see
    ``abort_hold``'s docstring for the reboot boundary this module actually
    has. It does not block an operator manually starting ``cratedigger.service``
    during this window, though; the drain below is what catches that.

    The drain *after* the hold is ``SERVICE_UNITS`` -- every unit this
    module knows about, not just ``GATE_STOPPED_UNITS`` (the ones abort
    explicitly restarts). ``cratedigger.service`` is itself gate-guarded, so
    the hold should already have stopped it if it was running -- exactly as
    it stops web/preview/importer/YouTube -- but nothing before this
    verified that for main specifically. This pre-hold window is the one
    place this module ever leaves ``cratedigger.service`` without a start
    inhibitor -- ``prepare_controlled`` establishes one for its own
    post-release window, precisely because a manual start or a foreign
    hold's ``resume-if-clear`` would otherwise start main there too. Here,
    the timer mask alone does not cover either: it blocks only the timer
    trigger, not a manual ``systemctl start`` or an unrelated hold's
    ``resume-if-clear``. Without re-checking it here, acquire could reach
    HELD with a main cycle still active if it was started that way during
    the queue-drain wait above -- and the migration runs between
    ``acquire`` and ``verify-held`` (#1078 BLOCKER F3). Re-verifying the
    three timer-driven producers here too is redundant in the ordinary case
    (the first drain already proved them inactive) but free: nothing
    between the two drains restarts them.
    """
    _drain_services(
        backend, TIMER_DRIVEN_PRODUCER_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )
    _wait_automation_queue_drained(backend)
    _ensure_owned_manual_hold(backend)
    _drain_services(
        backend, SERVICE_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )


def acquire_hold(backend: DeployHoldBackend) -> None:
    """Create or resume an authoritative strict hold acquisition."""
    backend.ensure_control_dir()
    backend.verify_controlled_start_contract()
    if backend.receipt_exists():
        _require_phase(backend, PHASE_ACQUIRING)
    else:
        if backend.manual_hold_active():
            if backend.persistent_manual_marker_exists():
                raise DeployHoldError(
                    "manual hold already exists and is marked as ours from "
                    "a previous deploy hold (a reboot likely lost the "
                    "receipt) -- run 'abort' to adopt and release it, then "
                    "re-acquire"
                )
            raise DeployHoldError("unowned manual hold already exists")
        for service in START_INHIBITORS:
            if backend.inhibitor_exists(service):
                if backend.persistent_inhibitor_marker_exists(service):
                    raise DeployHoldError(
                        f"producer inhibitor already exists for {service} "
                        "and is marked as ours from a previous deploy hold "
                        "(a reboot likely lost the receipt) -- run 'abort' "
                        "to adopt and release it, then re-acquire"
                    )
                raise DeployHoldError(
                    f"unowned producer inhibitor already exists for {service}"
                )
        for timer in TIMER_UNITS:
            target = backend.control_link_target(timer)
            if target is not None:
                raise DeployHoldError(
                    f"unowned control path already exists for {timer}: {target!r}"
                )
        backend.create_receipt()

    _mask_and_stop_timers(backend)
    _drain_producers_then_hold(backend)
    _assert_clean_old_lifecycle(backend)
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
    _mask_and_stop_timers(backend)
    if phase == PHASE_ACQUIRING:
        # An acquiring receipt has never reached HELD, so recovery must
        # re-prove exactly what acquire_hold proves, in the same
        # producer-drain-before-hold order -- otherwise recovery reintroduces
        # the exact #1078 deadlock (the gate hold stopping the importer
        # before the automation queue it drains ever gets a chance to).
        #
        # create_receipt() persists PHASE_ACQUIRING before acquire_hold reaches
        # either precondition, so an acquiring receipt has never proven them.
        # Recovery must re-prove exactly what acquire_hold proves before it may
        # promote that receipt to the HELD boundary verify_held and
        # prepare_controlled trust. Proving last mirrors acquire_hold: a
        # failure leaves the strictest boundary re-established and the receipt
        # authoritatively acquiring.
        _drain_producers_then_hold(backend)
        backend.verify_controlled_start_contract()
        _assert_clean_old_lifecycle(backend)
    else:
        # Every later phase already proved both before the switch that runs the
        # migration this hold gates. The preflight is deliberately NOT re-run
        # there: post-migration it reads a schema and a lifecycle the controlled
        # cycle has legitimately moved on, so re-proving it could only brick a
        # recovery that exists to restore safety.
        _ensure_owned_manual_hold(backend)
        _drain_services(
            backend, SERVICE_UNITS,
            timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
        )
        _clear_owned_inhibitors(backend)
    # Clear a previously captured ordinary-successor identity only once the
    # branch above has actually re-proven the full HELD boundary -- not
    # unconditionally up front (this function's prior shape). This is
    # forward hygiene, not a live escape hatch: a recover_held that fails
    # mid-branch already re-masks every timer (_mask_and_stop_timers runs
    # unconditionally, above, before this branch) and, in the else branch,
    # may already own the manual hold again, so complete_release is refused
    # by its own manual-hold/owned-link checks regardless of whether the
    # marker survives -- no reachable command consumes it today. Clearing
    # early just destroyed state before the boundary it belongs to was
    # re-proven, for no operator-visible benefit. write_ordinary_invocation's
    # replace=False is why some clear is still needed here at all: a later
    # finish_release, redone from a fresh HELD boundary, must be able to
    # write a new marker without tripping over a stale one this recovery
    # leaves behind.
    backend.clear_ordinary_invocation()
    backend.write_phase(PHASE_HELD)


def _validate_no_unowned_deploy_hold_conflicts(backend: DeployHoldBackend) -> None:
    """Fail closed on any unowned object abort would need to touch or trust.

    Checked in full before abort mutates anything, so a refusal here always
    leaves the receipt exactly as it was found -- never half-dismantled
    (#1078 MUST FIX 4). ``_clear_owned_inhibitors`` already carries an
    equivalent per-service check, but discovering it mid-teardown -- after
    the manual hold is already released and workers already started -- would
    leave abort stuck: it can neither finish releasing (the conflict is
    still there) nor cleanly go back to HELD (``recover_held`` hits the same
    conflict re-taking the hold). Proving it first removes that stuck state
    by construction.

    A foreign metadata gate hold (any reason other than our own owned
    ``manual`` one) gets the same up-front treatment whenever abort is about
    to attempt a gate-guarded restart -- releasing the owned manual hold, or
    removing an owned producer-start inhibitor. Without this, abort could
    release our manual hold, discover the foreign hold only afterward (inside
    the restart-verification wait), and exit non-zero with our hold already
    gone: the receipt still claims ``held`` while nothing blocks the foreign
    hold's own eventual ``resume-if-clear`` from starting every guarded unit,
    including a main cycle, underneath it (#1078 BLOCKER F1).

    The ``manual`` reason is excluded from ``foreign_reasons`` only when this
    receipt actually owns the manual hold -- never merely because *something*
    here is owned (#1096 correction round, S1). A hold coincidentally named
    ``manual`` that this receipt does NOT own is exactly as foreign as any
    other reason: every gate-guarded unit's ``ExecCondition`` checks that the
    *entire* holds directory is empty, not just that no reason other than
    ``manual`` is present, so a same-named-but-unowned hold would otherwise
    pass this check, let an owned inhibitor's file be deleted, and only then
    discover -- deep inside the restart-verification wait -- that nothing it
    starts can actually come up.
    """
    manual_hold_owned = backend.manual_hold_is_owned()
    if manual_hold_owned or backend.owned_inhibitor_units():
        foreign_reasons = tuple(
            reason
            for reason in backend.metadata_hold_reasons()
            if not (manual_hold_owned and reason == METADATA_MANUAL_HOLD.name)
        )
        if foreign_reasons:
            raise DeployHoldError(
                f"foreign metadata gate holds block abort: {foreign_reasons!r}"
            )
    for service in START_INHIBITORS:
        if backend.inhibitor_exists(service) and not backend.inhibitor_is_owned(service):
            raise DeployHoldError(
                f"unowned producer inhibitor exists for {service}"
            )
    for timer in TIMER_UNITS:
        target = backend.control_link_target(timer)
        if target is not None and not backend.link_is_owned(timer):
            raise DeployHoldError(
                f"unowned control path exists for {timer}: {target!r}"
            )


def _validate_no_unowned_persistent_conflicts(
    backend: DeployHoldBackend,
    *,
    manual_marked: bool,
    inhibited_marked: tuple[str, ...],
) -> None:
    """Mirror ``_validate_no_unowned_deploy_hold_conflicts`` for adoption.

    No receipt exists in this branch, so there is no tmpfs authority to
    consult -- ownership is exactly the set of persistent markers the caller
    already discovered. Checked in full before adoption mutates anything, so
    a refusal here leaves every marker and object exactly as found for a
    rerun once the conflict clears. Timer control-links need no equivalent
    check here: this function's precondition is exactly "no receipt", not
    "a reboot happened", so a control-link mask could in principle still be
    present for a wholly unrelated reason -- but there is no persistent
    marker for a control-link, so adoption owns none and deliberately never
    inspects or touches one, surviving or not. Validating and reclaiming an
    unowned control path remains solely ``acquire``'s job, the next time it
    runs.

    The ``manual`` reason is excluded from ``foreign_reasons`` only when
    ``manual_marked`` is true -- never merely because *something* here is
    marked (#1096 correction round, S1; mirrors the identical fix in
    ``_validate_no_unowned_deploy_hold_conflicts``). A hold coincidentally
    named ``manual`` that we do not hold the persistent marker for is
    exactly as foreign as any other reason: every gate-guarded unit's
    ``ExecCondition`` checks that the *entire* holds directory is empty, so
    a same-named-but-unmarked hold would otherwise pass this check, let a
    marked inhibitor's file be deleted, and only then discover -- deep
    inside the restart-verification wait -- that nothing it starts can
    actually come up.
    """
    if manual_marked or inhibited_marked:
        foreign_reasons = tuple(
            reason
            for reason in backend.metadata_hold_reasons()
            if not (manual_marked and reason == METADATA_MANUAL_HOLD.name)
        )
        if foreign_reasons:
            raise DeployHoldError(
                f"foreign metadata gate holds block abort: {foreign_reasons!r}"
            )
    for service in START_INHIBITORS:
        if backend.inhibitor_exists(service) and service not in inhibited_marked:
            raise DeployHoldError(
                f"unowned producer inhibitor exists for {service}"
            )


def _adopt_persistent_markers_or_refuse(backend: DeployHoldBackend) -> bool:
    """Receiptless abort: adopt exactly what our persistent markers own.

    Reached only once ``abort_hold`` has already confirmed there is no live
    or retired receipt to work from -- the shape a reboot leaves behind
    (#1096): ``/run/cratedigger-deploy-hold`` and every tmpfs ownership
    marker it held are gone, but the manual gate hold and the producer start
    inhibitors under ``/var/lib/cratedigger-metadata-gate`` are real disk
    state and can outlive the receipt that took them, together with the
    persistent sibling marker each one carries (written before the object
    itself, per ``mark_manual_hold_owned`` / ``mark_inhibitor_owned``).

    Returns ``False`` -- touching nothing -- when no persistent marker
    exists at all, so a clean boot with no prior deploy hold refuses exactly
    like today rather than mass-restarting every held unit. Every conflict
    this world could contain is proven absent up front
    (``_validate_no_unowned_persistent_conflicts``), before any mutation, so
    a refusal there leaves every marker and object exactly as found.

    Ordering is deliberate and load-bearing (#1096 correction round, M1/M2):

    - every marked inhibitor is removed FIRST, before anything below tries
      to (re)start a gate-guarded unit. A still-present inhibitor
      condition-skips its own unit's start (systemd exit 0, unit stays
      down) -- if the manual hold is *also* marked (reachable from a reboot
      mid ``prepare_controlled``, after both inhibitors are created but
      before the manual hold is released), releasing the hold and starting
      ``GATE_STOPPED_UNITS`` -- which includes YouTube ingest -- before
      YouTube's own inhibitor is gone would silently skip its start and
      hang the proof waiting for it forever;
    - the restart-and-prove step excludes ``MAIN_SERVICE`` even when it is
      marked (reachable from a reboot mid ``prepare_controlled``, after
      both inhibitors are created but before ``MAIN_SERVICE``'s own is
      released again near the end of that function). ``MAIN_SERVICE`` is
      ``Type=oneshot``: it runs one cycle and exits, so it can never satisfy
      ``_wait_controlled_workers_active``'s active/running proof -- waiting
      for it there would hang for the full drain timeout every single time,
      inhibitor files already deleted, before failing and leaving the
      markers stranded for an identical rerun. It is started separately,
      with no active-wait, exactly like ``prepare_controlled``'s own
      handling (``_release_owned_inhibitor(MAIN_SERVICE)`` immediately
      followed by a bare ``start_unit``, never a wait).

    Every marker is removed only after this function has done everything it
    is ever going to do for the object it names -- proven active for
    everything but ``MAIN_SERVICE``, attempted at least once for
    ``MAIN_SERVICE`` -- so an interrupted retry recomputes the identical
    marked set and safely repeats whatever remains idempotent (removing an
    already-absent inhibitor file, starting an already-active unit).
    Adoption never re-establishes a receipt or a phase -- there is nothing
    here to recover TO, only ordinary unheld operation to restore.
    """
    manual_marked = backend.persistent_manual_marker_exists()
    inhibited_marked = tuple(
        service
        for service in START_INHIBITORS
        if backend.persistent_inhibitor_marker_exists(service)
    )
    if not manual_marked and not inhibited_marked:
        return False

    _validate_no_unowned_persistent_conflicts(
        backend, manual_marked=manual_marked, inhibited_marked=inhibited_marked
    )

    for service in inhibited_marked:
        if backend.inhibitor_exists(service):
            backend.remove_start_inhibitor(service)

    if manual_marked and backend.manual_hold_active():
        backend.metadata_gate("release manual")
        if backend.manual_hold_active():
            raise DeployHoldError(
                "metadata gate did not release the marked manual hold"
            )

    restart_and_prove: set[str] = set(GATE_STOPPED_UNITS) if manual_marked else set()
    restart_and_prove |= set(inhibited_marked)
    restart_and_prove.discard(MAIN_SERVICE)
    if restart_and_prove:
        ordered = tuple(sorted(restart_and_prove))
        for service in ordered:
            backend.start_unit(service)
        _wait_controlled_workers_active(backend, ordered)

    if manual_marked:
        backend.metadata_gate("resume-if-clear")
        if reasons := backend.metadata_hold_reasons():
            raise DeployHoldError(
                f"metadata gate retained holds after adoption resume: {reasons!r}"
            )
        backend.remove_persistent_manual_marker()

    if MAIN_SERVICE in inhibited_marked:
        backend.start_unit(MAIN_SERVICE)

    for service in inhibited_marked:
        backend.remove_persistent_inhibitor_marker(service)

    return True


def abort_hold(backend: DeployHoldBackend) -> None:
    """Release every receipt-owned object and remove an incomplete receipt.

    The way out of a hold that ``recover_held`` cannot rescue: an acquire
    the strict old-lifecycle preflight or the controlled-start contract keeps
    refusing forever (nothing will ever fix an anomaly field, and
    ``recover_held`` on an acquiring receipt re-proves the identical
    preconditions), or a SIGINT or dropped SSH that left the process
    interrupted while the host stayed up. Where every other command in this
    module re-proves or advances the strict boundary, ``abort`` is the one
    that walks away from it -- back to ordinary, unheld operation -- for a
    receipt that cannot or should not proceed.

    A host reboot clears ``/run`` -- the receipt and every tmpfs ownership
    marker (manual hold, producer inhibitors, timer control-links) -- but
    the manual gate hold and the producer start inhibitors under
    ``/var/lib/cratedigger-metadata-gate`` are real disk state and can
    outlive the receipt that took them (#1096). Each carries its own
    persistent sibling marker, written before the object itself and removed
    only after it, so a receiptless ``abort`` can prove which surviving
    objects are ours (``_adopt_persistent_markers_or_refuse``) and adopt
    exactly those -- releasing the manual hold and/or producer inhibitors it
    still owns, restarting and proving active whatever they blocked (except
    ``MAIN_SERVICE``, which is ``Type=oneshot`` and is only ever proven
    attempted, never active -- see that function's own docstring), then
    clearing the markers -- ending at the same ordinary, unheld operation
    every other path through this function reaches. It never
    re-establishes a receipt or a phase: there is nothing to recover TO
    after a reboot, only ordinary operation to restore. Timer control-links
    need no such marker: this receiptless path's own precondition is
    exactly "no receipt", not "a reboot happened", so a control-link mask
    could in principle still be present for an unrelated reason -- but
    there is no persistent marker for one, so adoption owns none and
    deliberately never inspects or touches one either way; reclaiming an
    unowned control path remains solely ``acquire``'s job. A foreign hold
    or an unmarked inhibitor is
    refused exactly as it is under a live receipt, with every marker
    retained for a rerun once the conflict clears. With no receipt and no
    persistent marker at all -- an ordinary clean boot -- ``abort`` still
    refuses, so a boot with no prior deploy hold can never turn into a mass
    restart. ``acquire``'s own refusal for an object carrying one of these
    markers names ``abort`` as the way out; ``recover_held`` still requires
    a receipt, because the phase knowledge a reboot destroys is exactly what
    it exists to resume -- the supported reboot recovery is always ``abort``
    followed by a fresh ``acquire``.

    Every ownership class this receipt could hold is validated up front,
    before any mutation (``_validate_no_unowned_deploy_hold_conflicts``), so
    a refusal here never leaves the boundary half torn down. It then walks
    the same ownership markers acquisition records intent through and
    releases exactly the ones this receipt owns, in the reverse of the order
    acquisition took them -- disowning each only once this function has done
    everything it is ever going to do for the object that ownership implies
    it stopped: proven stably active for every unit that can reach that
    state, or attempted at least once for ``MAIN_SERVICE`` (``Type=oneshot``,
    which never can) -- so an interrupted retry never sees "nothing owned"
    while the underlying object is still down:

    - the manual gate hold, if owned -- releasing it is what the external
      gate tool consults to let every gate-guarded unit start again, so
      abort restarts all four (web, preview, importer, and YouTube ingest --
      itself gate-guarded since #1078 MUST FIX 1) and proves every one is
      stably active, the same way ``prepare_controlled`` does, before
      trusting the release and disowning the hold. A foreign hold (for
      example the monthly discogs-import hold) makes that proof fail loudly
      instead of ``abort`` silently exiting 0 with every worker still down;
    - every owned producer-start inhibitor -- restarted and proven active,
      except ``MAIN_SERVICE``'s, which is started with no active-wait
      (#1096 correction round, M1: a receipt can own both inhibitors
      together, interrupted mid ``prepare_controlled``, and waiting for the
      oneshot to reach active/running would hang forever);
    - every owned timer control-link mask -- releasing it restarts that
      timer, which is what returns ``cratedigger-unfindable.service`` and
      the watchdog to their ordinary cadence, and ``cratedigger.service``
      too if nothing has restarted it already. Unlike the other two, main
      is also named in the metadata gate's own ``resume_units`` (see
      ``verify_controlled_start_contract``'s ``expected_resume``) -- so a
      ``resume-if-clear`` call, the one above or one raised by an unrelated
      hold's release, can and does start it directly, independent of its
      timer. ``cratedigger-unfindable.service`` and the watchdog are absent
      from ``resume_units``, so they really are only ever timer-triggered.

    It never adopts or mutates an object this receipt did not itself own --
    the same ownership discipline every other command in this module already
    follows.
    """
    if not backend.receipt_exists():
        if backend.retired_receipt_exists():
            # Mirrors complete_release: THIS receipt's own removal was
            # interrupted after the atomic retirement rename but before
            # clearing the retired directory -- finishing that retirement
            # is everything left owned by IT. A wholly unrelated, earlier
            # incident's orphaned persistent marker(s) (#1096 correction
            # round, N6) are a real but separate possibility this branch
            # deliberately does not also adopt in the same run: retirement
            # finishes first, and a rerun of abort -- now hitting neither
            # this branch nor a receipt -- reaches
            # _adopt_persistent_markers_or_refuse below on its own. Two
            # runs, not a dead end.
            backend.finish_retired_receipt()
            return
        if _adopt_persistent_markers_or_refuse(backend):
            return
        raise DeployHoldError(
            "deploy hold receipt is missing and no persistent ownership "
            "markers exist to adopt"
        )
    phase = backend.read_phase()
    known_phases = {
        PHASE_ACQUIRING,
        PHASE_HELD,
        PHASE_PREPARED_CONTROLLED,
        PHASE_MAIN_TIMER_OPEN,
        PHASE_COMPLETE_PENDING,
    }
    if phase not in known_phases:
        raise DeployHoldError(f"cannot abort unknown phase: {phase!r}")

    _validate_no_unowned_deploy_hold_conflicts(backend)

    # Every owned inhibitor is removed FIRST, before anything below tries to
    # (re)start a gate-guarded unit -- the same M2 ordering fix adoption
    # needs (#1096 correction round), for the identical reason: a
    # still-present inhibitor condition-skips its own unit's start (exit 0,
    # unit stays down). A receipt can own the manual hold and an inhibitor
    # together -- interrupted mid ``prepare_controlled``, after both
    # inhibitors are created but before the manual hold is released -- and
    # releasing the hold and restarting ``GATE_STOPPED_UNITS`` (which
    # includes YouTube ingest) before YouTube's own inhibitor is gone would
    # silently skip its start and hang the proof below forever.
    owned_inhibited_services = backend.owned_inhibitor_units()
    for service in owned_inhibited_services:
        if not backend.inhibitor_is_owned(service):
            raise DeployHoldError(
                f"refusing to remove unowned producer inhibitor: {service}"
            )
        if backend.inhibitor_exists(service):
            backend.remove_start_inhibitor(service)

    if backend.manual_hold_is_owned():
        if backend.manual_hold_active():
            backend.metadata_gate("release manual")
            if backend.manual_hold_active():
                raise DeployHoldError(
                    "metadata gate did not release the owned manual hold"
                )
        for service in GATE_STOPPED_UNITS:
            backend.start_unit(service)
        _wait_controlled_workers_active(backend, GATE_STOPPED_UNITS)
        backend.metadata_gate("resume-if-clear")
        if reasons := backend.metadata_hold_reasons():
            raise DeployHoldError(
                f"metadata gate retained holds after abort resume: {reasons!r}"
            )
        backend.unmark_manual_hold_owned()

    # #1078 BLOCKER F2: at prepared-controlled/main-timer-open, the manual
    # hold is already unowned (prepare_controlled released it), so the block
    # above never runs -- but an owned YouTube inhibitor can still be the
    # only thing stopping cratedigger-youtube-ingest. Removing a
    # ConditionPathExists inhibitor does not start a stopped Type=simple
    # unit; finish_release's own release-then-resume shape only works
    # because it is the LAST inhibitor released, immediately before a
    # metadata_gate("resume-if-clear") that starts every remaining guarded
    # unit. abort has no such following resume, so it must restart and prove
    # each service its own inhibitor release unblocks, exactly like the
    # manual-hold branch does for GATE_STOPPED_UNITS, before disowning it --
    # except MAIN_SERVICE (#1096 correction round, M1 mirror): a receipt can
    # own MAIN_SERVICE's own inhibitor too (the same interrupted-mid
    # prepare_controlled window as above, this time caught after the manual
    # hold is released but before prepare_controlled's own
    # ``_release_owned_inhibitor(MAIN_SERVICE)`` runs). MAIN_SERVICE is
    # Type=oneshot -- it runs one cycle and exits, so it can never satisfy
    # ``_wait_controlled_workers_active``'s active/running proof; it is
    # started separately below with no active-wait, exactly like
    # ``prepare_controlled``'s own handling.
    restart_and_prove_inhibited = tuple(
        service for service in owned_inhibited_services if service != MAIN_SERVICE
    )
    if restart_and_prove_inhibited:
        for service in restart_and_prove_inhibited:
            backend.start_unit(service)
        _wait_controlled_workers_active(backend, restart_and_prove_inhibited)
    if MAIN_SERVICE in owned_inhibited_services:
        backend.start_unit(MAIN_SERVICE)
    for service in owned_inhibited_services:
        backend.unmark_inhibitor_owned(service)

    owned_timers = backend.owned_link_units()
    for timer in owned_timers:
        if not backend.link_is_owned(timer):
            raise DeployHoldError(f"refusing to remove unowned control link: {timer}")
        target = backend.control_link_target(timer)
        if target is not None and target != "/dev/null":
            raise DeployHoldError(f"owned control link changed for {timer}: {target!r}")
        if target is not None:
            backend.remove_control_mask(timer)
    if owned_timers:
        backend.daemon_reload()
        _assert_load_states(backend, masked=(), loaded=owned_timers)
        for timer in owned_timers:
            backend.start_unit(timer)
        for timer in owned_timers:
            state = backend.unit_state(timer)
            if state.active_state != "active":
                raise DeployHoldError(
                    f"restarted timer is not active for {timer}: {state.active_state}"
                )
        for timer in owned_timers:
            backend.unmark_link_owned(timer)

    backend.remove_receipt()


def prepare_controlled(backend: DeployHoldBackend) -> None:
    """Retain every timer mask while starting one controlled main cycle."""
    _require_phase(backend, PHASE_HELD)
    _verify_authoritative_hold(backend)
    for service in START_INHIBITORS:
        _ensure_owned_start_inhibitor(backend, service)
    backend.metadata_gate("release manual")
    if backend.manual_hold_active():
        raise DeployHoldError("metadata gate did not release the owned manual hold")
    backend.unmark_manual_hold_owned()
    for service in CONTROLLED_WORKER_UNITS:
        backend.start_unit(service)
    _wait_controlled_workers_active(backend)
    backend.metadata_gate("resume-if-clear")
    if reasons := backend.metadata_hold_reasons():
        raise DeployHoldError(
            f"metadata gate retained holds after controlled resume: {reasons!r}"
        )
    _assert_owned_links(backend, TIMER_UNITS)
    backend.daemon_reload()
    _assert_load_states(backend, masked=TIMER_UNITS, loaded=())
    _wait_controlled_workers_active(backend)
    _drain_services(
        backend, (MAIN_SERVICE, YOUTUBE_SERVICE),
        timeout_seconds=_DRAIN_TIMEOUT_SECONDS,
    )
    _release_owned_inhibitor(backend, MAIN_SERVICE)
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
    _drain_services(
        backend, PRODUCER_SERVICE_UNITS,
        timeout_seconds=_PRODUCER_DRAIN_TIMEOUT_SECONDS,
    )
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
    if backend.owned_inhibitor_units() != (YOUTUBE_SERVICE,):
        raise DeployHoldError(
            "release requires exactly the receipt-owned YouTube inhibitor"
        )
    if not backend.inhibitor_exists(YOUTUBE_SERVICE):
        raise DeployHoldError("owned YouTube inhibitor is missing before release")
    if backend.control_link_target(MAIN_TIMER) is not None:
        raise DeployHoldError("main timer control path reappeared before release")
    backend.write_ordinary_invocation(ordinary_invocation)
    for timer in (UNFINDABLE_TIMER, WATCHDOG_TIMER):
        _release_owned_link(backend, timer)
    backend.daemon_reload()
    _assert_load_states(backend, masked=(), loaded=TIMER_UNITS)
    for timer in (UNFINDABLE_TIMER, WATCHDOG_TIMER):
        backend.start_unit(timer)
    _release_owned_inhibitor(backend, YOUTUBE_SERVICE)
    if backend.metadata_gate("resume-if-clear") != 0:
        raise DeployHoldError(
            "metadata gate did not resume after every boundary was released"
        )
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
    if backend.owned_inhibitor_units():
        raise DeployHoldError(
            "owned producer inhibitors remain at release completion"
        )
    for service in START_INHIBITORS:
        if backend.inhibitor_exists(service):
            raise DeployHoldError(
                f"producer inhibitor exists at release completion for {service}"
            )
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
        "abort",
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
        elif args.command == "abort":
            abort_hold(backend)
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
