"""Deterministic contract pins for scripts/pin_nixosconfig.sh."""

from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from tests.fakes.deploy_pin import FakeDeployPinCommands
from tests.structural_audits.deploy_pin import find_shell_contract_violations


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "pin_nixosconfig.sh"
SKILL = REPO_ROOT / ".claude" / "skills" / "deploy" / "SKILL.md"


class TestDeployPinShellContractAudit(unittest.TestCase):
    def test_production_entrypoint_is_explicit_bash_and_clean(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        self.assertEqual(source.splitlines()[0], "#!/usr/bin/env bash")
        self.assertEqual(find_shell_contract_violations(source), ())
        self.assertLess(source.index("flock 9"),
                        source.index("worktree add --detach"))

    def test_real_unquoted_git_format_shape_is_rejected(self) -> None:
        bad = 'test "$(git log -1 --format=%G?)" = G\n'
        self.assertIn(
            "unquoted --format=%G?",
            find_shell_contract_violations(bad),
        )

    def test_real_zsh_readonly_status_shape_is_rejected(self) -> None:
        bad = "cleanup_on_exit() { local status=$?; }\n"
        self.assertIn(
            "local status=$?",
            find_shell_contract_violations(bad),
        )

    def test_skill_invokes_entrypoint_instead_of_copying_state_machine(self) -> None:
        source = SKILL.read_text(encoding="utf-8")
        self.assertIn("scripts/pin_nixosconfig.sh", source)
        self.assertNotIn("worktree add --detach", source)
        self.assertNotIn("GIT_CONFIG_VALUE_0", source)


class TestDeployPinScript(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.fake = FakeDeployPinCommands(Path(self.tempdir.name))

    def test_success_updates_only_cratedigger_and_verifies_remote(self) -> None:
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["remote_target"], self.fake.TARGET_REV)
        self.assertEqual(state["remote_rev"], state["receipt_rev"])
        self.assertEqual(state["commit_count"], 1)
        self.assertIn(["nix", "flake", "update", "cratedigger-src"],
                      state["events"])
        self.assertIn(["ls-remote"], state["events"])
        self.assertIsNone(state["worktree"])
        self.assertIn("signed nixosconfig revision", proc.stdout)
        for call in state["argv_calls"]:
            self.assertNotIn("test-secret-token", " ".join(call))
            self.assertNotIn("Authorization:", " ".join(call))

    def test_concurrent_same_target_invocations_create_one_pin(self) -> None:
        self.fake.update_state(nix_delay_seconds=0.25)
        first = self.fake.popen(SCRIPT)
        time.sleep(0.05)
        second = self.fake.popen(SCRIPT)
        first_stdout, first_stderr = first.communicate(timeout=20)
        second_stdout, second_stderr = second.communicate(timeout=20)
        state = self.fake.state

        self.assertEqual(first.returncode, 0, first_stderr)
        self.assertEqual(second.returncode, 0, second_stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["remote_rev"], state["receipt_rev"])
        self.assertEqual(
            sum(event[0] == "worktree-add" for event in state["events"]),
            1,
        )
        combined_stdout = first_stdout + second_stdout
        self.assertIn("signed nixosconfig revision", combined_stdout)
        self.assertIn("remote already at pending revision", combined_stdout)

    def test_failure_before_push_cleans_up_without_receipt(self) -> None:
        self.fake.update_state(fault="nix")
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["remote_rev"], self.fake.BASE_REV)
        self.assertIsNone(state["receipt_rev"])
        self.assertIsNone(state["worktree"])
        self.assertFalse(any(event[0] == "push" for event in state["events"]))

    def test_push_rejection_retains_and_reuses_exact_signed_commit(self) -> None:
        self.fake.update_state(fault="push")
        first = self.fake.run(SCRIPT)
        pending = self.fake.state["receipt_rev"]
        self.assertNotEqual(first.returncode, 0)
        self.assertIsNotNone(pending)

        self.fake.clear_fault()
        second = self.fake.run(SCRIPT)
        state = self.fake.state
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["remote_rev"], pending)
        self.assertIn(f"recovering pending revision: {pending}", second.stdout)

    def test_signature_failure_never_creates_recovery_ref_or_pushes(self) -> None:
        self.fake.update_state(fault="signature")
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertIsNone(state["receipt_rev"])
        self.assertFalse(any(event[0] == "push" for event in state["events"]))
        self.assertIsNone(state["worktree"])

    def test_cleanup_failure_reports_recoverable_remote_revision(self) -> None:
        self.fake.update_state(fault="cleanup")
        first = self.fake.run(SCRIPT)
        state_after_failure = self.fake.state
        intended = state_after_failure["receipt_rev"]

        self.assertNotEqual(first.returncode, 0)
        self.assertEqual(state_after_failure["remote_rev"], intended)
        self.assertIn(str(intended), first.stderr)

        self.fake.clear_fault()
        second = self.fake.run(SCRIPT)
        state = self.fake.state
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertIn(f"remote already at pending revision: {intended}",
                      second.stdout)

    def test_incompatible_remote_advancement_fails_with_all_coordinates(self) -> None:
        self.fake.update_state(fault="push")
        first = self.fake.run(SCRIPT)
        state = self.fake.state
        pending = state["receipt_rev"]
        parent = state["commits"][pending]["parent"]
        self.assertNotEqual(first.returncode, 0)

        self.fake.update_state(fault=None, remote_rev=self.fake.OTHER_REV,
                               remote_target=self.fake.OLD_TARGET)
        second = self.fake.run(SCRIPT)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn(f"pending={pending}", second.stderr)
        self.assertIn(f"base={parent}", second.stderr)
        self.assertIn(f"remote={self.fake.OTHER_REV}", second.stderr)
        self.assertEqual(self.fake.state["commit_count"], 1)

    def test_compatible_remote_advancement_allows_the_next_target(self) -> None:
        first = self.fake.run(SCRIPT)
        receipt = self.fake.state["receipt_rev"]
        self.assertEqual(first.returncode, 0, first.stderr)

        self.fake.update_state(
            remote_rev=self.fake.OTHER_REV,
            remote_target=self.fake.TARGET_REV,
            remote_ancestors=[receipt],
        )
        next_target = "5" * 40
        second = self.fake.run(SCRIPT, target=next_target)
        state = self.fake.state

        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(state["commit_count"], 2)
        self.assertEqual(state["remote_target"], next_target)
        self.assertEqual(state["commits"][state["receipt_rev"]]["parent"],
                         self.fake.OTHER_REV)

    def test_same_target_retry_accepts_signed_descendant_containing_pin(self) -> None:
        first = self.fake.run(SCRIPT)
        receipt = self.fake.state["receipt_rev"]
        self.assertEqual(first.returncode, 0, first.stderr)

        self.fake.update_state(
            remote_rev=self.fake.OTHER_REV,
            remote_target=self.fake.TARGET_REV,
            remote_ancestors=[receipt],
        )
        second = self.fake.run(SCRIPT)

        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(self.fake.state["commit_count"], 1)
        self.assertIn(f"current={self.fake.OTHER_REV}", second.stdout)
        self.assertIn(f"signed nixosconfig revision: {self.fake.OTHER_REV}",
                      second.stdout)

    def test_existing_remote_lock_at_target_is_reported_without_commit(self) -> None:
        self.fake.update_state(remote_target=self.fake.TARGET_REV)
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["commit_count"], 0)
        self.assertFalse(any(event[0] == "nix" for event in state["events"]))
        self.assertIn(f"already pins {self.fake.TARGET_REV}", proc.stdout)

    def test_non_forgejo_origin_fails_before_fetch_or_token_read(self) -> None:
        self.fake.update_state(
            origin_url="https://github.com/abl030/nixosconfig.git"
        )
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("origin must be https://git.ablz.au", proc.stderr)
        self.assertFalse(any(event[0] == "fetch" for event in state["events"]))
        self.assertFalse(any(event[0] == "ls-remote"
                             for event in state["events"]))


if __name__ == "__main__":
    unittest.main()
