"""Deterministic contracts for the exact Cratedigger cycle verifier."""

from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from tests._source_pins import pinned_source
from tests.fakes.deploy_cycle import FakeDeployCycleCommands

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "verify_cratedigger_cycle.sh"
SKILL = REPO_ROOT / ".claude" / "skills" / "deploy" / "SKILL.md"


class TestDeployCycleVerifier(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDeployCycleCommands(Path(self.tempdir.name))

    def test_capture_current_returns_exact_invocation(self) -> None:
        proc = self.fake.run(SCRIPT, "capture-current")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.OLD)

    def test_capture_current_uses_none_for_empty_invocation(self) -> None:
        self.fake.write_state(
            system_states=[self.fake.system_state("", active="inactive", sub="dead")],
            journal_snapshots={},
        )

        proc = self.fake.run(SCRIPT, "capture-current")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), "none")

    def test_capture_cursor_returns_exact_journal_boundary(self) -> None:
        proc = self.fake.run(SCRIPT, "capture-cursor")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.CURSOR)

    def test_forced_command_agent_key_cannot_consume_verification_ssh(self) -> None:
        """Issue #837: verification must never offer the forwarded agent."""
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={
                self.fake.TARGET: [self.fake.success_records()],
            },
            forced_agent_present=True,
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.fake.state["forced_command_hits"], 0)
        ssh_events = self.fake.state["events"]
        self.assertTrue(ssh_events)
        for event in ssh_events:
            self.assertIn("IdentityAgent=none", event)

    def test_capture_target_ignores_old_source_then_returns_target(self) -> None:
        self.fake.write_state(
            system_states=[
                self.fake.system_state(self.fake.OLD),
                self.fake.system_state(self.fake.OLD_SUCCESSOR),
                self.fake.system_state(self.fake.TARGET),
            ],
            journal_snapshots={
                self.fake.OLD_SUCCESSOR: [[self.fake.source_record(
                    self.fake.OLD_SUCCESSOR,
                    source="/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-source",
                )]],
                self.fake.TARGET: [[self.fake.source_record()]],
            },
            start_journal_snapshots=[[
                self.fake.start_record(self.fake.OLD_SUCCESSOR),
                self.fake.start_record(self.fake.TARGET),
            ]],
        )

        proc = self.fake.run(
            SCRIPT,
            "capture-target",
            self.fake.CURSOR,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.TARGET)
        self.assertIn("ignoring invocation", proc.stderr)
        self.assertIn("from source", proc.stderr)

    def test_capture_target_cannot_skip_manager_only_failed_start(self) -> None:
        failed_target = [
            {
                "INVOCATION_ID": self.fake.TARGET,
                "JOB_RESULT": "failed",
                "JOB_TYPE": "start",
                "MESSAGE": "Failed to start Cratedigger.",
            },
        ]
        self.fake.write_state(
            system_states=[
                self.fake.system_state(self.fake.OLD),
                self.fake.system_state(self.fake.NEXT),
            ],
            journal_snapshots={
                self.fake.TARGET: [failed_target],
                self.fake.NEXT: [self.fake.success_records(
                    invocation=self.fake.NEXT,
                )],
            },
            start_journal_snapshots=[[
                self.fake.start_record(self.fake.TARGET),
                self.fake.start_record(self.fake.NEXT),
            ]],
        )

        proc = self.fake.run(
            SCRIPT,
            "capture-target",
            self.fake.CURSOR,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.TARGET)
        system_reads = [
            event for event in self.fake.state["events"]
            if "systemctl show cratedigger.service" in " ".join(event)
        ]
        self.assertEqual(system_reads, [])

    def test_wait_verifies_target_after_current_id_rolls_to_next(self) -> None:
        self.fake.write_state(
            system_states=[
                self.fake.system_state(self.fake.OLD),
                self.fake.system_state(self.fake.TARGET),
                self.fake.system_state(self.fake.NEXT),
            ],
            journal_snapshots={
                self.fake.TARGET: [
                    [self.fake.source_record()],
                    self.fake.success_records(),
                ],
            },
            start_journal_snapshots=[[
                self.fake.start_record(self.fake.TARGET),
            ]],
        )

        proc = self.fake.run(
            SCRIPT,
            "wait",
            self.fake.CURSOR,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(f"verified invocation {self.fake.TARGET}", proc.stdout)
        self.assertIn("after current unit rolled over", proc.stdout)

    def test_verify_exact_rereads_partial_journal_after_rollover(self) -> None:
        partial = [
            self.fake.source_record(),
            {
                "_SYSTEMD_INVOCATION_ID": self.fake.TARGET,
                "MESSAGE": "Cratedigger cycle complete in 1.0s",
            },
        ]
        self.fake.write_state(
            system_states=[
                self.fake.system_state(self.fake.NEXT),
                self.fake.system_state(self.fake.NEXT),
            ],
            journal_snapshots={
                self.fake.TARGET: [partial, self.fake.success_records()],
            },
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn("after current unit rolled over", proc.stdout)
        target_journal_reads = [
            event
            for event in self.fake.state["events"]
            if "journalctl" in " ".join(event)
            and f"--invocation={self.fake.TARGET}" in " ".join(event)
        ]
        self.assertEqual(len(target_journal_reads), 2)

    def test_verify_exact_rejects_explicit_target_failure(self) -> None:
        failed = [
            self.fake.source_record(),
            {
                "INVOCATION_ID": self.fake.TARGET,
                "JOB_RESULT": "failed",
                "JOB_TYPE": "start",
                "MESSAGE": "Failed to start Cratedigger — Soulseek download pipeline.",
            },
        ]
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={self.fake.TARGET: [failed]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("failed", proc.stderr)
        self.assertNotIn("incomplete", proc.stderr)

    def test_verify_exact_distinguishes_incomplete_rolled_target(self) -> None:
        incomplete = [
            self.fake.source_record(),
            {
                "_SYSTEMD_INVOCATION_ID": self.fake.TARGET,
                "MESSAGE": "Cratedigger cycle complete in 1.0s",
            },
        ]
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={self.fake.TARGET: [incomplete]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("incomplete", proc.stderr)
        self.assertIn("deactivated", proc.stderr)
        self.assertIn("finished", proc.stderr)

    def test_verify_exact_requires_manager_deactivated_success(self) -> None:
        records = [
            record
            for record in self.fake.success_records()
            if record.get("MESSAGE")
            != "cratedigger.service: Deactivated successfully."
        ]
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={self.fake.TARGET: [records]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("deactivated-success", proc.stderr)

    def test_verify_exact_requires_manager_finished_success(self) -> None:
        records = [
            record
            for record in self.fake.success_records()
            if record.get("MESSAGE")
            != "Finished Cratedigger — Soulseek download pipeline."
        ]
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={self.fake.TARGET: [records]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("finished-success", proc.stderr)

    def test_verify_exact_distinguishes_timeout_while_target_is_current(self) -> None:
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.TARGET)],
            journal_snapshots={self.fake.TARGET: [[self.fake.source_record()]]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
            max_polls=2,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("timed out", proc.stderr)
        self.assertNotIn("incomplete", proc.stderr)

    def test_source_match_is_an_exact_cmdline_token(self) -> None:
        wrong = self.fake.success_records(source=f"{self.fake.SOURCE}-old")
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.NEXT)],
            journal_snapshots={self.fake.TARGET: [wrong]},
        )

        proc = self.fake.run(
            SCRIPT,
            "verify-exact",
            self.fake.TARGET,
            self.fake.SOURCE,
        )

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("source", proc.stderr)

    def test_skill_calls_tracked_verifier_for_successor_cycle(self) -> None:
        source = pinned_source(SKILL)
        self.assertIn("scripts/verify_cratedigger_cycle.sh", source)
        self.assertIn("capture-current", source)
        self.assertIn("capture-cursor", source)
        self.assertIn("capture-target", source)
        self.assertIn("verify-exact", source)
        self.assertIn(
            "PRE_SWITCH_CRATEDIGGER_INVOCATION=%s\\n",
            source,
        )
        self.assertIn("POST_SWITCH_CRATEDIGGER_CURSOR=$(\n", source)
        self.assertIn(
            '"$POST_SWITCH_CRATEDIGGER_CURSOR" "$CRATEDIGGER_SOURCE"',
            source,
        )
        self.assertNotIn("<value printed by step 3>", source)
        step_six = source.index("6. Derive the active wrapper")
        source_check = source.index(
            "env -u SSH_AUTH_SOCK ssh doc2 \"grep '<something unique>'",
            step_six,
        )
        post_switch_capture = source.index(
            "POST_SWITCH_CRATEDIGGER_CURSOR=$(",
            step_six,
        )
        target_capture = source.index("TARGET_CRATEDIGGER_INVOCATION=$(", step_six)
        self.assertLess(source_check, post_switch_capture)
        self.assertLess(post_switch_capture, target_capture)

    def test_skill_runs_fleet_trigger_without_the_shared_agent(self) -> None:
        source = pinned_source(SKILL)

        self.assertIn("env -u SSH_AUTH_SOCK fleet-deploy doc2", source)
        for line in source.splitlines():
            if "ssh doc2" in line:
                self.assertIn("env -u SSH_AUTH_SOCK ssh doc2", line)


class TestMigrateRanForThisSwitch(unittest.TestCase):
    """#1161 — the deploy runbook must prove the migrate oneshot ran FOR THIS
    SWITCH, not merely that it is active.

    `cratedigger-db-migrate.service` is RemainAfterExit, so
    active/exited/success reads identically whether it ran seconds ago or
    12.5 hours ago. On 2026-08-14 a concurrent `systemctl start` replaced the
    switch's still-queued stop job, migration 078 never applied, and every
    documented state check passed against the stale run."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDeployCycleCommands(Path(self.tempdir.name))

    def _with_migrate(self, *migrate_states: dict[str, str]) -> None:
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.OLD)],
            journal_snapshots={},
            migrate_states=list(migrate_states),
        )

    def test_capture_migrate_returns_exact_invocation(self) -> None:
        proc = self.fake.run(SCRIPT, "capture-migrate")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.MIGRATE_OLD)

    def test_capture_migrate_uses_none_for_empty_invocation(self) -> None:
        self._with_migrate(
            self.fake.migrate_state("", active="inactive", sub="dead")
        )

        proc = self.fake.run(SCRIPT, "capture-migrate")

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), "none")

    def test_fresh_successful_invocation_verifies(self) -> None:
        self._with_migrate(self.fake.migrate_state(self.fake.MIGRATE_NEXT))

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertIn(self.fake.MIGRATE_NEXT, proc.stdout)

    def test_unchanged_invocation_fails_even_though_state_is_green(self) -> None:
        """The exact #1161 world: ActiveState=active, SubState=exited and
        Result=success all pass, but the unit never ran for this switch."""
        self._with_migrate(self.fake.migrate_state(self.fake.MIGRATE_OLD))

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("did not run for this switch", proc.stderr)

    def test_fresh_but_failed_invocation_fails(self) -> None:
        self._with_migrate(
            self.fake.migrate_state(
                self.fake.MIGRATE_NEXT, active="failed", sub="failed", result="exit-code"
            )
        )

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("did not succeed", proc.stderr)

    def test_still_running_migration_is_not_accepted(self) -> None:
        """A PRODUCIBLE world the state triple must reject. The runbook reads
        after an asynchronous fleet trigger, so it can catch the migrator
        mid-run: `activating`/`start`/`success` with a FRESH InvocationID
        passes the invocation comparison and must still fail closed. This is
        the world that makes the ActiveState and SubState clauses
        load-bearing — the all-fields-wrong world below cannot distinguish
        them."""
        self._with_migrate(
            self.fake.migrate_state(
                self.fake.MIGRATE_NEXT, active="activating", sub="start"
            )
        )

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("did not succeed", proc.stderr)

    def test_empty_pre_switch_invocation_is_rejected(self) -> None:
        proc = self.fake.run(SCRIPT, "verify-migrate-ran", "")

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("missing pre-switch", proc.stderr)

    def _failing_ssh(self, *fragments: str) -> None:
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.OLD)],
            journal_snapshots={},
            ssh_failures=list(fragments),
        )

    def test_failed_migrate_read_fails_closed_and_names_the_unit(self) -> None:
        """#1172 item 6. Until the fake could fail an ssh at all, the
        ``could not read ... state`` branch was unreachable from every test in
        this file, so a regression there — swallowing the failure and
        proceeding with an empty state — would have been invisible.

        Failing closed matters here specifically: an empty read yields an empty
        InvocationID, and the verifier must call that "never ran for this
        switch" rather than compare emptiness against the pre-switch value.
        """
        self._failing_ssh("systemctl show cratedigger-db-migrate.service")

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("could not read", proc.stderr)
        self.assertIn("cratedigger-db-migrate.service", proc.stderr)

    def test_failed_cratedigger_read_names_the_main_unit(self) -> None:
        """The same branch, reached through the other unit — proof the
        diagnostic is parameterised rather than hardcoded to either name."""
        self._failing_ssh("systemctl show cratedigger.service")

        proc = self.fake.run(SCRIPT, "capture-current")

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("could not read", proc.stderr)
        self.assertIn("cratedigger.service", proc.stderr)
        self.assertNotIn("cratedigger-db-migrate.service", proc.stderr)

    def test_unreadable_state_names_the_migrate_unit(self) -> None:
        """The diagnostic must name the unit actually queried. Before the unit
        parameter existed these messages were hardcoded to cratedigger.service,
        which would misdirect an operator debugging a migrate read."""
        self._with_migrate(
            {
                "InvocationID": self.fake.MIGRATE_NEXT,
                "ActiveState": "",
                "SubState": "",
                "Result": "success",
            }
        )

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("incomplete cratedigger-db-migrate.service state", proc.stderr)

    def test_missing_invocation_fails(self) -> None:
        self._with_migrate(
            self.fake.migrate_state("", active="inactive", sub="dead")
        )

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("never ran for this switch", proc.stderr)

    def test_first_ever_run_verifies_against_none(self) -> None:
        """A host with no prior migrate invocation captures `none`; the first
        real run must still verify."""
        self._with_migrate(self.fake.migrate_state(self.fake.MIGRATE_NEXT))

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", "none")

        self.assertEqual(proc.returncode, 0, proc.stderr)

    def test_missing_argument_is_a_usage_error(self) -> None:
        proc = self.fake.run(SCRIPT, "verify-migrate-ran")

        self.assertEqual(proc.returncode, 64)

    def test_malformed_pre_switch_invocation_is_rejected(self) -> None:
        proc = self.fake.run(SCRIPT, "verify-migrate-ran", "not-an-invocation")

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("invalid systemd InvocationID", proc.stderr)

    def test_skill_captures_before_the_trigger_and_verifies_after(self) -> None:
        """The runbook must capture the migrate invocation BEFORE
        `fleet-deploy` and assert with it after. Capturing after the switch
        would compare the post-switch value against itself and pass
        unconditionally — which is the whole failure #1161 describes."""
        source = pinned_source(SKILL)

        capture = source.index('verify_cratedigger_cycle.sh" capture-migrate')
        trigger = source.index("env -u SSH_AUTH_SOCK fleet-deploy doc2")
        # The executable invocation, not the bare token — prose mentioning the
        # subcommand must not be able to satisfy this ordering assertion.
        verify = source.index(
            'verify-migrate-ran "$PRE_SWITCH_MIGRATE_INVOCATION"'
        )

        self.assertLess(capture, trigger)
        self.assertLess(trigger, verify)

    def test_skill_no_longer_accepts_the_stale_state_triple_alone(self) -> None:
        """The superseded check read only ActiveState/SubState/Result, which a
        RemainAfterExit oneshot satisfies indefinitely."""
        source = pinned_source(SKILL)

        self.assertNotIn('test "$migration_active" = active', source)

    def test_migrate_reads_never_offer_the_forwarded_agent(self) -> None:
        """Issue #837's boundary covers the new SSH calls too."""
        self.fake.write_state(
            system_states=[self.fake.system_state(self.fake.OLD)],
            journal_snapshots={},
            migrate_states=[self.fake.migrate_state(self.fake.MIGRATE_NEXT)],
            forced_agent_present=True,
        )

        proc = self.fake.run(SCRIPT, "verify-migrate-ran", self.fake.MIGRATE_OLD)

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.fake.state["forced_command_hits"], 0)
        ssh_events = self.fake.state["events"]
        self.assertTrue(ssh_events)
        for event in ssh_events:
            self.assertIn("IdentityAgent=none", event)


class TestDeployCycleFakeShimCaching(unittest.TestCase):
    """Pins for the shared-module fake-command shape (issue #1156 item 5):
    the fake ``ssh`` is a tiny stub importing a shared ``_shim.py``, so
    CPython caches its compiled bytecode across every fake ssh invocation
    instead of recompiling on each one."""

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDeployCycleCommands(Path(self.tempdir.name))

    def fake_environment(self) -> dict[str, str]:
        env = os.environ.copy()
        env.update(
            {
                "PATH": f"{self.fake.fake_bin}:{env['PATH']}",
                "DEPLOY_CYCLE_FAKE_STATE": str(self.fake.state_path),
            }
        )
        return env

    def test_ssh_stub_is_tiny_and_shares_one_cached_shim_module(self) -> None:
        shim_path = self.fake.fake_bin / "_shim.py"
        self.assertTrue(shim_path.exists())
        shim_size = shim_path.stat().st_size
        stub_size = (self.fake.fake_bin / "ssh").stat().st_size
        # A regression back to writing the full body into the fake ssh
        # command (the pre-#1156-item-5 shape) would make the stub as
        # large as the shim itself.
        self.assertLess(stub_size, 300, "ssh stub is not tiny")
        self.assertLess(stub_size * 5, shim_size,
                         "ssh stub looks like a full shim copy")

        pycache = self.fake.fake_bin / "__pycache__"
        self.assertFalse(pycache.exists())

        proc = subprocess.run(
            [
                str(self.fake.fake_bin / "ssh"), "doc2", "systemctl", "show",
                "cratedigger-db-migrate.service", "--property=ActiveState",
            ],
            env=self.fake_environment(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), "ActiveState=active")

        cached = list(pycache.glob("_shim.*.pyc"))
        self.assertEqual(
            len(cached), 1,
            "expected the shim's bytecode to be cached in __pycache__ "
            f"after one call, found {cached}",
        )

    def test_ssh_stub_fails_loudly_without_the_shared_shim_module(self) -> None:
        (self.fake.fake_bin / "_shim.py").unlink()

        proc = subprocess.run(
            [
                str(self.fake.fake_bin / "ssh"), "doc2", "systemctl", "show",
                "cratedigger-db-migrate.service", "--property=ActiveState",
            ],
            env=self.fake_environment(),
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("ModuleNotFoundError", proc.stderr)
        self.assertIn("_shim", proc.stderr)


if __name__ == "__main__":
    unittest.main()
