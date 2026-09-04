"""Execution-lease, fail-stop session, and process-group primitives."""

import dataclasses
import os
import pathlib
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from itertools import pairwise
from typing import Self, get_type_hints
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(__file__))
import conftest  # noqa: F401

from album_source import DatabaseSource
from lib.fs_authority import remove_relative_tree
from lib.import_execution import (
    CancellationToken,
    CgroupObservation,
    CgroupState,
    ExecutionCancelled,
    ExecutionLeaseSnapshot,
    ExecutionLivenessEvidence,
    ExecutionOwnerProof,
    InvocationObservation,
    InvocationState,
    MonitoredProcessGroup,
    OwnerSessionIdentity,
    OwnerSessionProbe,
    OwnerSessionWatchdog,
    ProcessGroupTerminationError,
    ProcessIdentity,
    ProcessObservation,
    ProcessState,
    SystemExecutionLivenessProbe,
    cancellation_hook,
    capture_execution_lease,
    checkpoint,
    decide_execution_liveness,
    probe_execution_liveness,
)
from lib.pipeline_db import (
    ADVISORY_LOCK_NAMESPACE_IMPORT,
    ADVISORY_LOCK_NAMESPACE_RELEASE,
    PipelineDB,
)
from lib.pipeline_db._core import OwnerSessionLost, _PinnedOwnerSession

TEST_DSN = os.environ.get("TEST_DB_DSN") or ""


def _wait_for_cancellation(
    token: CancellationToken,
    timeout: float,
) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if token.cancelled:
            return True
        time.sleep(0.005)
    return token.cancelled


def _spawn_monitored(argv: list[str]) -> MonitoredProcessGroup:
    return MonitoredProcessGroup(subprocess.Popen(
        argv,
        start_new_session=True,
    ))


def _lease(*, child: bool = True) -> ExecutionLeaseSnapshot:
    return ExecutionLeaseSnapshot(
        host_boot_id="boot-a",
        invocation_id="invocation-a",
        systemd_unit="cratedigger-importer.service",
        worker=ProcessIdentity(pid=101, start_ticks=1001),
        beets=(
            ProcessIdentity(pid=202, start_ticks=2002)
            if child else None
        ),
    )


def _evidence(
    *,
    lease: ExecutionLeaseSnapshot | None = None,
    boot: str | None = "boot-a",
    boot_error: str | None = None,
    worker: ProcessState = "exact",
    child: ProcessState = "exact",
    invocation: InvocationState = "exact",
    cgroup: CgroupState = "exact",
) -> ExecutionLivenessEvidence:
    stored = _lease() if lease is None else lease
    return ExecutionLivenessEvidence(
        lease=stored,
        current_host_boot_id=boot,
        boot_error=boot_error,
        worker=ProcessObservation(
            identity=stored.worker,
            state=worker,
            observed_start_ticks=(
                stored.worker.start_ticks
                if worker == "exact"
                else stored.worker.start_ticks + 1
                if worker == "reused"
                else None
            ),
            cgroup_path=(
                "/system.slice/cratedigger-importer.service"
                if worker == "exact" else None
            ),
            reason=f"worker_{worker}",
        ),
        beets=(
            ProcessObservation(
                identity=stored.beets,
                state=child,
                observed_start_ticks=(
                    stored.beets.start_ticks
                    if child == "exact"
                    else stored.beets.start_ticks + 1
                    if child == "reused"
                    else None
                ),
                cgroup_path=(
                    "/system.slice/cratedigger-importer.service"
                    if child == "exact" else None
                ),
                reason=f"child_{child}",
            )
            if stored.beets is not None else None
        ),
        invocation=InvocationObservation(
            state=invocation,
            stored_invocation_id=stored.invocation_id,
            observed_invocation_id=(
                stored.invocation_id if invocation == "exact" else None
            ),
            control_group=(
                "/system.slice/cratedigger-importer.service"
                if invocation == "exact" else None
            ),
            reason=f"invocation_{invocation}",
        ),
        cgroup=CgroupObservation(
            state=cgroup,
            path=(
                "/system.slice/cratedigger-importer.service"
                if cgroup == "exact" else None
            ),
            member_pids=(101, 202) if cgroup == "exact" else (),
            reason=f"cgroup_{cgroup}",
        ),
    )


class TestExecutionLeaseCapture(unittest.TestCase):
    def test_captures_boot_invocation_unit_and_exact_process_identities(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            proc_root = root / "proc"
            proc_root.mkdir()
            boot_path = root / "boot_id"
            boot_path.write_text(" boot-a\n")
            for pid, ticks in ((101, 1001), (202, 2002)):
                proc_dir = proc_root / str(pid)
                proc_dir.mkdir()
                fields = ["S", *("0" for _ in range(18)), str(ticks)]
                (proc_dir / "stat").write_text(
                    f"{pid} (worker with spaces) {' '.join(fields)}\n"
                )

            lease = capture_execution_lease(
                systemd_unit="cratedigger-importer.service",
                invocation_id="invocation-a",
                worker_pid=101,
                beets_pid=202,
                proc_root=proc_root,
                boot_id_path=boot_path,
            )

        self.assertEqual(lease, _lease())

    def test_rejects_missing_invocation_and_blank_explicit_unit(self) -> None:
        with self.assertRaisesRegex(ValueError, "INVOCATION_ID"):
            capture_execution_lease(
                systemd_unit="cratedigger-importer.service",
                invocation_id="",
            )
        with self.assertRaisesRegex(ValueError, "systemd unit"):
            capture_execution_lease(
                systemd_unit=" ",
                invocation_id="invocation-a",
            )


class TestExecutionOwnerProof(unittest.TestCase):
    """The #898 owner pair, bundled for the completed-download lifecycle."""

    def test_holds_both_fields_and_compares_by_value(self) -> None:
        identity = OwnerSessionIdentity(connection_object_id=1, backend_pid=2)
        proof = ExecutionOwnerProof(
            execution_lease=_lease(),
            owner_session_identity=identity,
        )

        self.assertEqual(proof.execution_lease, _lease())
        self.assertIs(proof.owner_session_identity, identity)
        self.assertEqual(
            proof,
            ExecutionOwnerProof(
                execution_lease=_lease(),
                owner_session_identity=identity,
            ),
        )
        self.assertNotEqual(
            proof,
            ExecutionOwnerProof(
                execution_lease=_lease(child=False),
                owner_session_identity=identity,
            ),
        )

    def test_both_fields_are_required_with_no_default(self) -> None:
        """The whole point of this type: neither field is optional on its
        own, so a caller cannot construct a bundle naming an execution
        lease without also naming the session it expects to hold
        ``IMPORT``, or vice versa (review finding, WE8 mutant runner: this
        co-nullity claim had no direct test). Checked by introspection,
        not by a deliberately-wrong constructor call, so the pin does not
        need a Pyright escape hatch to assert the call would fail.

        ``field.type`` is not enough on its own: ``lib/import_execution.py``
        has ``from __future__ import annotations``, so
        ``dataclasses.fields()`` reports each field's annotation as an
        unevaluated string ('ExecutionLeaseSnapshot', not the class), and
        widening a field to ``X | None`` still has no default -- it stays
        "required" by this check's own no-default assertions while quietly
        admitting the exact ``None`` the whole bundle exists to forbid on
        one side. ``typing.get_type_hints`` resolves the string annotations
        into the real classes and is what actually catches that widening
        (Batch F, F4; issue #1355 residual triage round 2)."""
        field_names = {
            field.name: field for field in dataclasses.fields(ExecutionOwnerProof)
        }
        self.assertEqual(
            set(field_names), {"execution_lease", "owner_session_identity"},
        )
        for name, field in field_names.items():
            self.assertIs(
                field.default, dataclasses.MISSING,
                f"{name} must have no default — both fields are required",
            )
            self.assertIs(
                field.default_factory, dataclasses.MISSING,
                f"{name} must have no default_factory — both fields are required",
            )
        self.assertEqual(
            get_type_hints(ExecutionOwnerProof),
            {
                "execution_lease": ExecutionLeaseSnapshot,
                "owner_session_identity": OwnerSessionIdentity,
            },
            "a field resolving to anything other than its bare class "
            "(e.g. widened to `X | None`) defeats the required-together "
            "invariant this type exists to enforce",
        )

    def test_frozen_rejects_field_reassignment(self) -> None:
        proof = ExecutionOwnerProof(
            execution_lease=_lease(),
            owner_session_identity=OwnerSessionIdentity(
                connection_object_id=1, backend_pid=2,
            ),
        )

        # setattr, not direct assignment: pyright statically rejects direct
        # assignment to a frozen dataclass field, which would need a
        # typing-ratchet-tracked ignore here; setattr exercises the same
        # runtime __setattr__ override without one.
        with self.assertRaises(dataclasses.FrozenInstanceError):
            setattr(proof, "execution_lease", _lease(child=False))  # noqa: B010


class TestExecutionLivenessDecision(unittest.TestCase):
    def test_missing_lease_is_dead_never_claimed(self) -> None:
        decision = probe_execution_liveness(None)
        self.assertEqual(
            (decision.status, decision.reason),
            ("dead", "never_claimed"),
        )

    def test_changed_boot_is_conclusive_death(self) -> None:
        lease = _lease()
        decision = decide_execution_liveness(ExecutionLivenessEvidence(
            lease=lease,
            current_host_boot_id="boot-b",
            boot_error=None,
            worker=None,
            beets=None,
            invocation=None,
            cgroup=None,
        ))
        self.assertEqual((decision.status, decision.reason), ("dead", "boot_changed"))

    def test_same_boot_exact_identity_invocation_and_cgroup_is_live(self) -> None:
        decision = decide_execution_liveness(_evidence())
        self.assertEqual(
            (decision.status, decision.reason),
            ("live", "exact_execution_present"),
        )

    def test_positive_ended_invocation_cgroup_and_absent_pids_is_dead(
        self,
    ) -> None:
        decision = decide_execution_liveness(_evidence(
            worker="absent",
            child="absent",
            invocation="ended",
            cgroup="absent",
        ))
        self.assertEqual(
            (decision.status, decision.reason),
            ("dead", "execution_ended"),
        )

    def test_ended_invocation_cannot_override_an_exact_live_worker(
        self,
    ) -> None:
        decision = decide_execution_liveness(_evidence(
            lease=_lease(child=False),
            worker="exact",
            invocation="ended",
            cgroup="absent",
        ))
        self.assertEqual(decision.status, "unknown")

    def test_probe_failure_and_conflict_are_unknown(self) -> None:
        cases = (
            _evidence(worker="unknown"),
            _evidence(invocation="conflict", cgroup="conflict"),
        )
        for evidence in cases:
            with self.subTest(evidence=evidence):
                self.assertEqual(
                    decide_execution_liveness(evidence).status,
                    "unknown",
                )

    def test_pid_reuse_counts_as_stored_identity_absence(self) -> None:
        decision = decide_execution_liveness(_evidence(
            lease=_lease(child=False),
            worker="reused",
            invocation="ended",
            cgroup="absent",
        ))
        self.assertEqual(
            (decision.status, decision.reason),
            ("dead", "execution_ended"),
        )

    def test_probe_exception_fails_closed_with_evidence(self) -> None:
        probe = MagicMock()
        probe.observe.side_effect = PermissionError("cannot inspect cgroup")

        decision = probe_execution_liveness(_lease(), probe=probe)

        self.assertEqual(decision.status, "unknown")
        self.assertEqual(decision.reason, "probe_failed")
        self.assertIn("PermissionError", decision.evidence.probe_error or "")

    def test_real_proc_systemd_probe_proves_exact_execution_live(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            proc_root = root / "proc"
            proc_root.mkdir()
            boot_path = root / "boot_id"
            boot_path.write_text("boot-a\n")
            worker_dir = proc_root / "101"
            worker_dir.mkdir()
            fields = ["S", *("0" for _ in range(18)), "1001"]
            (worker_dir / "stat").write_text(
                f"101 (worker) {' '.join(fields)}\n"
            )
            (worker_dir / "cgroup").write_text(
                "0::/system.slice/cratedigger-importer.service\n"
            )
            probe = SystemExecutionLivenessProbe(
                proc_root=proc_root,
                boot_id_path=boot_path,
                cgroup_root=root / "cgroup",
            )
            systemctl = MagicMock(
                returncode=0,
                stdout=(
                    "InvocationID=invocation-a\n"
                    "ControlGroup=/system.slice/"
                    "cratedigger-importer.service\n"
                    "ActiveState=active\n"
                    "SubState=running\n"
                ),
            )

            with patch(
                "lib.import_execution.subprocess.run",
                return_value=systemctl,
            ):
                decision = probe_execution_liveness(
                    _lease(child=False),
                    probe=probe,
                )

        self.assertEqual(
            (decision.status, decision.reason),
            ("live", "exact_execution_present"),
        )

    def test_newer_systemd_invocation_proves_absent_old_execution_dead(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            proc_root = root / "proc"
            proc_root.mkdir()
            boot_path = root / "boot_id"
            boot_path.write_text("boot-a\n")
            probe = SystemExecutionLivenessProbe(
                proc_root=proc_root,
                boot_id_path=boot_path,
                cgroup_root=root / "cgroup",
            )
            systemctl = MagicMock(
                returncode=0,
                stdout=(
                    "InvocationID=invocation-new\n"
                    "ControlGroup=/system.slice/"
                    "cratedigger-importer.service\n"
                    "ActiveState=active\n"
                    "SubState=running\n"
                ),
            )

            with patch(
                "lib.import_execution.subprocess.run",
                return_value=systemctl,
            ):
                decision = probe_execution_liveness(
                    _lease(child=False),
                    probe=probe,
                )

        self.assertEqual(
            (decision.status, decision.reason),
            ("dead", "execution_ended"),
        )

    def test_retained_invocation_inactive_or_failed_is_ended_only_after(
        self,
    ) -> None:
        cases = (
            ("inactive", "dead", "", None, "dead"),
            (
                "failed",
                "failed",
                "/system.slice/cratedigger-importer.service",
                None,
                "dead",
            ),
            (
                "inactive",
                "dead",
                "/system.slice/cratedigger-importer.service",
                "303\n",
                "unknown",
            ),
        )
        for (
            active_state,
            sub_state,
            control_group,
            cgroup_members,
            expected_status,
        ) in cases:
            with self.subTest(active_state=active_state):
                with tempfile.TemporaryDirectory() as raw:
                    root = pathlib.Path(raw)
                    proc_root = root / "proc"
                    proc_root.mkdir()
                    boot_path = root / "boot_id"
                    boot_path.write_text("boot-a\n")
                    probe = SystemExecutionLivenessProbe(
                        proc_root=proc_root,
                        boot_id_path=boot_path,
                        cgroup_root=root / "cgroup",
                    )
                    if cgroup_members is not None:
                        cgroup_dir = (
                            root
                            / "cgroup"
                            / "system.slice"
                            / "cratedigger-importer.service"
                        )
                        cgroup_dir.mkdir(parents=True)
                        (cgroup_dir / "cgroup.procs").write_text(
                            cgroup_members
                        )
                    systemctl = MagicMock(
                        returncode=0,
                        stdout=(
                            "InvocationID=invocation-a\n"
                            f"ControlGroup={control_group}\n"
                            f"ActiveState={active_state}\n"
                            f"SubState={sub_state}\n"
                        ),
                    )
                    with patch(
                        "lib.import_execution.subprocess.run",
                        return_value=systemctl,
                    ):
                        decision = probe_execution_liveness(
                            _lease(child=False),
                            probe=probe,
                        )

                self.assertEqual(
                    decision.status,
                    expected_status,
                )


class TestCancellationTranslation(unittest.TestCase):
    """``checkpoint`` and ``cancellation_hook`` — the two shared translations.

    Both used to be spelled per module: five byte-identical ``_checkpoint``
    copies and four ``*_cancellable`` wrappers plus one inline conditional
    for the hook (issue #1313).
    """

    def test_checkpoint_without_a_token_does_nothing(self) -> None:
        checkpoint(None)

    def test_checkpoint_passes_an_uncancelled_token(self) -> None:
        checkpoint(CancellationToken())

    def test_checkpoint_refuses_a_cancelled_token(self) -> None:
        token = CancellationToken()
        token.cancel("owner_session_lost")
        with self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"):
            checkpoint(token)

    def test_no_token_yields_no_before_mutation_hook(self) -> None:
        self.assertIsNone(cancellation_hook(None))

    def test_the_hook_reads_the_token_when_called_not_when_taken(self) -> None:
        """A stage takes the hook once, then mutates for as long as it takes."""
        token = CancellationToken()
        hook = cancellation_hook(token)
        assert hook is not None
        hook()
        token.cancel("cancelled_mid_stage")
        with self.assertRaisesRegex(ExecutionCancelled, "cancelled_mid_stage"):
            hook()

    def test_the_hook_stops_a_real_tree_removal_before_it_mutates(self) -> None:
        """The consumer that matters: ``remove_relative_tree``'s own re-check."""
        token = CancellationToken()
        token.cancel("owner_session_lost")
        with tempfile.TemporaryDirectory() as raw:
            os.mkdir(os.path.join(raw, "doomed"))
            pathlib.Path(raw, "doomed", "track.flac").write_bytes(b"audio")
            parent_fd = os.open(raw, os.O_RDONLY | os.O_DIRECTORY)
            try:
                with self.assertRaisesRegex(
                    ExecutionCancelled, "owner_session_lost",
                ):
                    remove_relative_tree(
                        parent_fd,
                        "doomed",
                        before_mutation=cancellation_hook(token),
                    )
            finally:
                os.close(parent_fd)
            self.assertTrue(pathlib.Path(raw, "doomed", "track.flac").exists())

    def test_no_hook_lets_the_same_removal_run(self) -> None:
        """Must-still-work: an uncancellable caller still deletes the tree."""
        with tempfile.TemporaryDirectory() as raw:
            os.mkdir(os.path.join(raw, "doomed"))
            pathlib.Path(raw, "doomed", "track.flac").write_bytes(b"audio")
            parent_fd = os.open(raw, os.O_RDONLY | os.O_DIRECTORY)
            try:
                remove_relative_tree(
                    parent_fd, "doomed", before_mutation=cancellation_hook(None),
                )
            finally:
                os.close(parent_fd)
            self.assertFalse(pathlib.Path(raw, "doomed").exists())


class TestCancellationAndProcessGroup(unittest.TestCase):
    def test_first_cancellation_reason_wins(self) -> None:
        token = CancellationToken()
        self.assertTrue(token.cancel("session_lost"))
        self.assertFalse(token.cancel("later_reason"))
        self.assertEqual(token.reason, "session_lost")
        with self.assertRaisesRegex(ExecutionCancelled, "session_lost"):
            token.raise_if_cancelled()

    def test_cancelled_wait_terminates_and_waits_for_real_process_group(
        self,
    ) -> None:
        token = CancellationToken()
        process = _spawn_monitored(
            [sys.executable, "-c", "import time; time.sleep(30)"],
        )
        self.assertEqual(os.getpgid(process.pid), process.pid)
        token.cancel("owner_session_lost")

        with self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"):
            process.wait(token, probe_interval=0.05)

        self.assertIsNotNone(process.returncode)

    def test_failed_owner_probe_cancels_and_terminates_group(self) -> None:
        token = CancellationToken()
        process = _spawn_monitored(
            [sys.executable, "-c", "import time; time.sleep(30)"],
        )

        with self.assertRaisesRegex(ExecutionCancelled, "owner_session_lost"):
            process.wait(
                token,
                owner_session_probe=lambda: False,
                probe_interval=0.05,
            )

        self.assertTrue(token.cancelled)
        self.assertIsNotNone(process.returncode)

    def test_owner_session_watchdog_cancels_the_whole_stage(self) -> None:
        token = CancellationToken()
        probes = iter((True, False))

        with OwnerSessionWatchdog(
            token,
            lambda _deadline: next(probes),
            probe_interval=0.01,
        ):
            self.assertTrue(_wait_for_cancellation(token, 1.0))

        self.assertEqual(token.reason, "owner_session_lost")

    def test_watchdog_stop_waits_for_a_blocked_probe_to_release(self) -> None:
        token = CancellationToken()
        entered = threading.Event()
        release = threading.Event()
        stopped = threading.Event()

        def blocked_probe(_deadline: float) -> bool:
            entered.set()
            release.wait()
            return True

        watchdog = OwnerSessionWatchdog(
            token,
            blocked_probe,
            probe_interval=0.05,
        )
        watchdog.start()
        self.assertTrue(entered.wait(1.0))
        stopper = threading.Thread(
            target=lambda: (watchdog.stop(), stopped.set()),
        )
        stopper.start()
        self.assertFalse(stopped.wait(0.05))
        release.set()
        stopper.join(timeout=1.0)
        self.assertTrue(stopped.is_set())

    def test_watchdog_probe_cadence_includes_probe_elapsed_time(self) -> None:
        token = CancellationToken()
        starts: list[float] = []

        def slow_probe(_deadline: float) -> bool:
            starts.append(time.monotonic())
            time.sleep(0.075)
            if len(starts) == 3:
                token.cancel("cadence_observed")
            return True

        watchdog = OwnerSessionWatchdog(
            token,
            slow_probe,
            probe_interval=0.1,
        )
        watchdog.start()
        self.assertTrue(_wait_for_cancellation(token, 1.0))
        watchdog.stop()

        self.assertEqual(len(starts), 3)
        self.assertTrue(
            all(
                later - earlier < 0.145
                for earlier, later in pairwise(starts)
            ),
            starts,
        )

    def test_cancellation_racing_zero_exit_still_wins(self) -> None:
        token = CancellationToken()
        child = MagicMock()
        child.wait.side_effect = lambda timeout: (
            token.cancel("session_lost_during_exit"),
            0,
        )[1]
        process = MonitoredProcessGroup(child)

        with patch.object(
            process,
            "terminate_and_wait",
            return_value=-signal.SIGKILL,
        ) as terminate, self.assertRaisesRegex(
            ExecutionCancelled,
            "session_lost_during_exit",
        ):
            process.wait(token, probe_interval=0.05)

        terminate.assert_called_once_with()

    def test_termination_kills_sigterm_resistant_descendant_after_leader(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            ready = pathlib.Path(raw) / "descendant.pid"
            descendant = (
                "import os,pathlib,signal,sys,time;"
                "signal.signal(signal.SIGTERM, signal.SIG_IGN);"
                "pathlib.Path(sys.argv[1]).write_text(str(os.getpid()));"
                "time.sleep(30)"
            )
            leader = (
                "import subprocess,sys,time;"
                f"subprocess.Popen([sys.executable,'-c',{descendant!r},"
                "sys.argv[1]]);"
                "time.sleep(30)"
            )
            process = _spawn_monitored(
                [sys.executable, "-c", leader, str(ready)],
            )
            deadline = time.monotonic() + 2.0
            while not ready.exists() and time.monotonic() < deadline:
                threading.Event().wait(0.01)
            self.assertTrue(ready.exists())
            descendant_pid = int(ready.read_text())

            returncode = process.terminate_and_wait(timeout=3.0)

            self.assertIsNotNone(returncode)
            with self.assertRaises(ProcessLookupError):
                os.kill(descendant_pid, 0)

    def test_unproven_group_absence_raises_fail_stop_error(self) -> None:
        child = MagicMock()
        child.pid = 424242
        child.poll.return_value = 0
        process = MonitoredProcessGroup(child)

        with patch("lib.import_execution.os.killpg"), patch.object(
            process,
            "_group_exists",
            return_value=True,
        ), self.assertRaises(ProcessGroupTerminationError):
            process.terminate_and_wait(timeout=0.02)


class TestPinnedOwnerSessionPostgres(unittest.TestCase):
    def setUp(self) -> None:
        self.assertTrue(TEST_DSN, "conftest must set TEST_DB_DSN")

    def test_backend_is_stable_and_loss_never_reconnects_inside_pin(
        self,
    ) -> None:
        db = PipelineDB(TEST_DSN)
        killer = PipelineDB(TEST_DSN)
        token = CancellationToken()
        try:
            with db._pin_owner_session(token) as identity:
                original_conn = db.conn
                row = db._execute(
                    "SELECT pg_backend_pid() AS backend_pid"
                ).fetchone()
                self.assertEqual(int(row["backend_pid"]), identity.backend_pid)
                self.assertTrue(db._probe_owner_session(identity).live)

                killed = killer._execute(
                    "SELECT pg_terminate_backend(%s) AS killed",
                    (identity.backend_pid,),
                ).fetchone()
                self.assertTrue(killed["killed"])
                probe = db._probe_owner_session(identity)

                self.assertFalse(probe.live)
                self.assertTrue(token.cancelled)
                self.assertIs(db.conn, original_conn)
                with self.assertRaises(OwnerSessionLost):
                    db._execute("SELECT 1")
                self.assertIs(db.conn, original_conn)

            row = db._execute(
                "SELECT pg_backend_pid() AS backend_pid"
            ).fetchone()
            self.assertNotEqual(int(row["backend_pid"]), identity.backend_pid)
        finally:
            killer.close()
            db.close()

    def test_watchdog_start_failure_clears_and_closes_pin(self) -> None:
        db = PipelineDB(TEST_DSN)
        token = CancellationToken()
        try:
            with patch(
                "lib.import_execution.threading.Thread",
                side_effect=RuntimeError("thread unavailable"),
            ), self.assertRaisesRegex(
                OwnerSessionLost,
                "watchdog could not start",
            ), db._pin_owner_session(token):
                self.fail("pin scope must not start")

            self.assertEqual(
                token.reason,
                "owner_session_watchdog_start_failed",
            )
            self.assertIsNone(db._owner_session_pin)
            self.assertNotEqual(db.conn.closed, 0)
        finally:
            db.close()

    def test_cancelled_waiter_cannot_begin_advisory_lock_statement(
        self,
    ) -> None:
        class GateLock:
            def __init__(self) -> None:
                self.entered = threading.Event()
                self.release = threading.Event()

            def __enter__(self) -> Self:
                self.entered.set()
                self.release.wait()
                return self

            def __exit__(
                self,
                t: type[BaseException] | None,
                v: BaseException | None,
                tb: object,
            ) -> None:
                return None

        db = PipelineDB(TEST_DSN)
        token = CancellationToken()
        gate = GateLock()
        outcome: list[str] = []
        identity = OwnerSessionIdentity(
            connection_object_id=id(db.conn),
            backend_pid=db.conn.get_backend_pid(),
        )
        db._owner_session_pin = _PinnedOwnerSession(
            db.conn,
            identity,
            token,
            gate,
        )

        def acquire() -> None:
            try:
                with db.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_IMPORT,
                    999_898,
                ):
                    outcome.append("statement_executed")
            except OwnerSessionLost:
                outcome.append("cancelled")

        waiter = threading.Thread(target=acquire)
        try:
            waiter.start()
            self.assertTrue(gate.entered.wait(1.0))
            token.cancel("cancelled_while_waiting_for_io")
            gate.release.set()
            waiter.join(timeout=1.0)
            self.assertFalse(waiter.is_alive())
            self.assertEqual(outcome, ["cancelled"])
        finally:
            gate.release.set()
            waiter.join(timeout=1.0)
            db._owner_session_pin = None
            db.close()

    def test_replacing_connection_object_inside_pin_fails_closed(self) -> None:
        db = PipelineDB(TEST_DSN)
        replacement = PipelineDB(TEST_DSN)
        token = CancellationToken()
        original = db.conn
        try:
            with db._pin_owner_session(token) as identity:
                db.conn = replacement.conn
                probe = db._probe_owner_session(identity)
                self.assertFalse(probe.live)
                self.assertEqual(probe.reason, "connection_replaced")
                self.assertTrue(token.cancelled)
                with self.assertRaises(OwnerSessionLost):
                    db._execute("SELECT 1")
        finally:
            db.conn = original
            replacement.close()
            db.close()

    def test_cancelled_owner_token_prevents_more_sql_on_healthy_pin(
        self,
    ) -> None:
        db = PipelineDB(TEST_DSN)
        token = CancellationToken()
        try:
            with db._pin_owner_session(token) as identity:
                token.cancel("stage_cancelled")
                self.assertEqual(
                    db._probe_owner_session(identity).reason,
                    "execution_cancelled",
                )
                with self.assertRaisesRegex(
                    OwnerSessionLost,
                    "owner execution is cancelled",
                ):
                    db._execute("SELECT 1")
                self.assertEqual(
                    db.conn.get_backend_pid(),
                    identity.backend_pid,
                )
        finally:
            db.close()

    def test_probe_deadline_bounds_a_stopped_postgres_backend(self) -> None:
        db = PipelineDB(TEST_DSN)
        token = CancellationToken()
        backend_pid: int | None = None
        try:
            with db._pin_owner_session(token) as identity:
                backend_pid = identity.backend_pid
                # Let the automatic watchdog's immediate first probe finish,
                # so this assertion measures the explicit 150 ms deadline
                # rather than waiting behind that probe's 750 ms lock scope.
                self.assertTrue(db._probe_owner_session(identity).live)
                os.kill(backend_pid, signal.SIGSTOP)
                started = time.monotonic()
                probe = db._probe_owner_session(
                    identity,
                    deadline_seconds=0.15,
                )
                elapsed = time.monotonic() - started

                self.assertFalse(probe.live)
                self.assertTrue(token.cancelled)
                self.assertLess(elapsed, 0.5)
        finally:
            if backend_pid is not None:
                try:
                    os.kill(backend_pid, signal.SIGCONT)
                except ProcessLookupError:
                    pass
            db.close()

    def test_pin_is_not_released_while_watchdog_probe_is_running(
        self,
    ) -> None:
        db = PipelineDB(TEST_DSN)
        token = CancellationToken()
        probe_entered = threading.Event()
        release_probe = threading.Event()
        exit_complete = threading.Event()

        def blocked_probe(
            identity: OwnerSessionIdentity,
            *,
            deadline_seconds: float,
        ) -> OwnerSessionProbe:
            self.assertLessEqual(deadline_seconds, 1.0)
            probe_entered.set()
            release_probe.wait()
            return OwnerSessionProbe(
                True,
                "exact_backend",
                identity,
                identity.backend_pid,
            )

        try:
            with patch.object(
                db,
                "_probe_owner_session",
                side_effect=blocked_probe,
            ):
                pin = db._pin_owner_session(token)
                pin.__enter__()
                self.assertTrue(probe_entered.wait(1.0))
                exiting = threading.Thread(
                    target=lambda: (
                        pin.__exit__(None, None, None),
                        exit_complete.set(),
                    ),
                )
                exiting.start()
                self.assertFalse(exit_complete.wait(0.05))
                self.assertIsNotNone(db._owner_session_pin)
                release_probe.set()
                exiting.join(timeout=1.0)
                self.assertTrue(exit_complete.is_set())
                self.assertIsNone(db._owner_session_pin)
        finally:
            release_probe.set()
            db.close()

    def test_import_then_release_and_borrowed_source_use_exact_backend(
        self,
    ) -> None:
        db = PipelineDB(TEST_DSN)
        observer = PipelineDB(TEST_DSN)
        token = CancellationToken()
        lock_key = 898_003
        try:
            with db._pin_owner_session(token) as identity, db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                lock_key,
            ) as import_acquired:
                self.assertTrue(import_acquired)
                with db.advisory_lock(
                    ADVISORY_LOCK_NAMESPACE_RELEASE,
                    lock_key,
                ) as release_acquired:
                    self.assertTrue(release_acquired)
                    row = db._execute(
                        "SELECT pg_backend_pid() AS backend_pid"
                    ).fetchone()
                    self.assertEqual(
                        int(row["backend_pid"]),
                        identity.backend_pid,
                    )
                    with observer.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_IMPORT,
                        lock_key,
                    ) as other_import:
                        self.assertFalse(other_import)
                    with observer.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_RELEASE,
                        lock_key,
                    ) as other_release:
                        self.assertFalse(other_release)

                    source = DatabaseSource(
                        TEST_DSN,
                        musicbrainz_ws2_base="http://mb.invalid/ws/2",
                        discogs_api_base="http://discogs.invalid",
                        borrowed_db=db,
                    )
                    self.assertIs(source._get_db(), db)
                    source.close()
                    self.assertTrue(
                        db._probe_owner_session(identity).live
                    )
                    with observer.advisory_lock(
                        ADVISORY_LOCK_NAMESPACE_IMPORT,
                        lock_key,
                    ) as after_source_close:
                        self.assertFalse(after_source_close)

            with observer.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                lock_key,
            ) as released:
                self.assertTrue(released)
        finally:
            observer.close()
            db.close()

    def test_killed_atomic_owner_session_rolls_back_without_replay(
        self,
    ) -> None:
        db = PipelineDB(TEST_DSN)
        observer = PipelineDB(TEST_DSN)
        token = CancellationToken()
        lock_key = 898_004
        table = "test_owner_session_atomic"
        observer._execute(f"DROP TABLE IF EXISTS {table}")
        observer._execute(
            f"CREATE TABLE {table} (marker TEXT PRIMARY KEY)"
        )
        try:
            with db._pin_owner_session(token) as identity, db.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                lock_key,
            ) as acquired:
                self.assertTrue(acquired)
                with self.assertRaises(OwnerSessionLost), db._atomic():
                    db._execute(
                        f"INSERT INTO {table} (marker) VALUES (%s)",
                        ("before-loss",),
                    )
                    killed = observer._execute(
                        "SELECT pg_terminate_backend(%s) AS killed",
                        (identity.backend_pid,),
                    ).fetchone()
                    self.assertTrue(killed["killed"])
                    db._execute(
                        f"INSERT INTO {table} (marker) VALUES (%s)",
                        ("must-not-replay",),
                    )

            rows = observer._execute(
                f"SELECT marker FROM {table}"
            ).fetchall()
            self.assertEqual(rows, [])
            self.assertTrue(token.cancelled)
            with observer.advisory_lock(
                ADVISORY_LOCK_NAMESPACE_IMPORT,
                lock_key,
            ) as lock_released:
                self.assertTrue(lock_released)
        finally:
            observer._execute(f"DROP TABLE IF EXISTS {table}")
            observer.close()
            db.close()
