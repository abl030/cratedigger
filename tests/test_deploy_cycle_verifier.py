"""Deterministic contracts for the exact Cratedigger cycle verifier."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

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
                    source="/nix/store/old-source",
                )]],
                self.fake.TARGET: [[self.fake.source_record()]],
            },
        )

        proc = self.fake.run(
            SCRIPT,
            "capture-target",
            self.fake.OLD,
            self.fake.SOURCE,
        )

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(proc.stdout.strip(), self.fake.TARGET)
        self.assertIn("ignoring non-target-source invocation", proc.stderr)

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
        )

        proc = self.fake.run(
            SCRIPT,
            "wait",
            self.fake.OLD,
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
        source = SKILL.read_text(encoding="utf-8")
        self.assertIn("scripts/verify_cratedigger_cycle.sh", source)
        self.assertIn("capture-current", source)
        self.assertIn("capture-target", source)
        self.assertIn("verify-exact", source)
        self.assertIn(
            "PRE_SWITCH_CRATEDIGGER_INVOCATION=%s\\n",
            source,
        )
        self.assertIn(
            "POST_SWITCH_CRATEDIGGER_INVOCATION=$(\n"
            '  "$CRATEDIGGER_REPO/scripts/verify_cratedigger_cycle.sh" '
            "capture-current\n)",
            source,
        )
        self.assertIn(
            '"$POST_SWITCH_CRATEDIGGER_INVOCATION" "$CRATEDIGGER_SOURCE"',
            source,
        )
        self.assertNotIn("<value printed by step 3>", source)
        step_six = source.index("6. Derive the active wrapper")
        source_check = source.index("ssh doc2 \"grep '<something unique>'", step_six)
        post_switch_capture = source.index(
            "POST_SWITCH_CRATEDIGGER_INVOCATION=$(",
            step_six,
        )
        target_capture = source.index("TARGET_CRATEDIGGER_INVOCATION=$(", step_six)
        self.assertLess(source_check, post_switch_capture)
        self.assertLess(post_switch_capture, target_capture)


if __name__ == "__main__":
    unittest.main()
