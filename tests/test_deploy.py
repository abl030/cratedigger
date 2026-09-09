"""Deterministic contract pins for scripts/deploy.sh."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tests._source_pins import pinned_source
from tests.fakes.deploy import (
    NEW_INVOCATION,
    PREVIOUS_INVOCATION,
    FakeDeployWorld,
    upgrade_state,
)
from tests.structural_audits.deploy import find_shell_contract_violations

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "deploy.sh"
SKILL = REPO_ROOT / ".claude" / "skills" / "deploy" / "SKILL.md"


class TestDeployShellContract(unittest.TestCase):
    def test_entrypoint_is_explicit_bash_and_clean(self) -> None:
        raw = SCRIPT.read_text(encoding="utf-8")
        self.assertEqual(raw.splitlines()[0], "#!/usr/bin/env bash")
        self.assertEqual(find_shell_contract_violations(raw), ())
        # Presence is asserted on the comment-stripped source (#1172/#1186):
        # a commented-out line must not satisfy the pin.
        live = pinned_source(SCRIPT)
        self.assertIn("set -euo pipefail", live)
        # The fleet trigger key must never reach the shared agent, and a
        # wedged forwarded agent hangs commit signing.
        self.assertIn("unset SSH_AUTH_SOCK", live)

    def test_real_unquoted_git_format_shape_is_rejected(self) -> None:
        bad = 'test "$(git log -1 --format=%G?)" = G\n'
        self.assertIn("unquoted --format=%G?", find_shell_contract_violations(bad))

    def test_real_zsh_readonly_status_shape_is_rejected(self) -> None:
        bad = "cleanup_on_exit() { local status=$?; }\n"
        self.assertIn("local status=$?", find_shell_contract_violations(bad))

    def test_skill_runs_the_script_and_carries_no_runbook(self) -> None:
        """The skill invokes the one entrypoint; the state machine lives in
        the script, never copied back into prose an agent re-narrates."""
        self.assertIn("scripts/deploy.sh", pinned_source(SKILL))
        # Absence is asserted on the raw text: a copied step hiding behind a
        # comment marker is still a copied step.
        raw = SKILL.read_text(encoding="utf-8")
        for copied_step in (
            "worktree add",
            "GIT_CONFIG_VALUE_0",
            "fleet-deploy doc2",
            "verify-migrate-ran",
            "last-verified-rev",
        ):
            self.assertNotIn(copied_step, raw)


class TestDeployScript(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.world = FakeDeployWorld(Path(self.tempdir.name))

    def _event_names(self) -> list[object]:
        return [event[0] for event in self.world.events()]

    def test_default_target_pins_origin_main_signed_and_activates(self) -> None:
        proc = self.world.run()

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.world.forgejo_master_pins(), self.world.main_tip)
        self.assertEqual(self.world.forgejo_master_signature(), "G")
        self.assertEqual(self.world.forgejo_master_changed_paths(), ["flake.lock"])
        self.assertEqual(
            self.world.git(self.world.forgejo_bare, "rev-parse", "refs/heads/master^"),
            self.world.nixosconfig_base,
        )
        pushed = self.world.forgejo_master()
        self.assertIn(f"doc2 activated nixosconfig {pushed} = cratedigger "
                      f"{self.world.main_tip}", proc.stdout)
        self.assertIn("next timer cycle", proc.stdout)
        names = self._event_names()
        self.assertEqual(names.count("fleet-deploy"), 1)
        self.assertEqual(names.count("nix"), 1)
        # The pin is pushed and read back through the token boundary only.
        self.assertEqual(
            [event[1] for event in self.world.events() if event[0] == "forgejo-auth"],
            ["git-push", "git-ls-remote"],
        )
        # The temporary worktree is gone; only the checkout itself remains.
        self.assertEqual(len(self.world.nixosconfig_worktrees()), 1)

    def test_no_edge_ever_sees_the_shared_agent(self) -> None:
        proc = self.world.run()

        self.assertEqual(proc.returncode, 0, proc.stderr)
        events = self.world.events()
        self.assertGreater(len(events), 5)
        for event in events:
            self.assertFalse(event[-1], f"SSH_AUTH_SOCK reached {event[:2]!r}")

    def test_explicit_revision_on_main_is_pinned_exactly(self) -> None:
        older = self.world.cratedigger_revisions[1]

        proc = self.world.run(older)

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.world.forgejo_master_pins(), older)
        self.assertIn(f"cratedigger {older}", proc.stdout)

    def test_revision_off_main_is_refused_before_pinning(self) -> None:
        proc = self.world.run("d" * 40)

        self.assertEqual(proc.returncode, 1)
        self.assertIn("is not on origin/main", proc.stderr)
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)
        self.assertNotIn("nix", self._event_names())
        self.assertNotIn("fleet-deploy", self._event_names())

    def test_malformed_revision_is_a_usage_error(self) -> None:
        proc = self.world.run("main")

        self.assertEqual(proc.returncode, 2)
        self.assertIn("full 40-hex SHA", proc.stderr)

    def test_already_pinned_master_triggers_without_a_new_commit(self) -> None:
        pinned = self.world.pin_forgejo_master(self.world.main_tip)

        proc = self.world.run()

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.world.forgejo_master(), pinned)
        self.assertIn(f"nixosconfig master {pinned} already pins it", proc.stdout)
        self.assertNotIn("nix", self._event_names())
        self.assertNotIn("forgejo-auth", self._event_names())
        self.assertEqual(self._event_names().count("fleet-deploy"), 1)
        self.assertIn(f"doc2 activated nixosconfig {pinned}", proc.stdout)

    def test_in_flight_upgrade_is_waited_out_before_the_trigger(self) -> None:
        """A running upgrade absorbs a trigger into its own job; the script
        lets it finish so the trigger mints a fresh invocation."""
        self.world.update_state(
            previous_active_states=["activating", "active", "inactive"],
        )

        proc = self.world.run()

        self.assertEqual(proc.returncode, 0, proc.stderr)
        # Announced once, not once per poll.
        self.assertEqual(proc.stdout.count("already running on doc2; waiting"), 1)
        names = self._event_names()
        active_reads = [
            index for index, event in enumerate(self.world.events())
            if event[0] == "ssh" and "--property=ActiveState --value" in str(event[2])
        ]
        self.assertGreaterEqual(len(active_reads), 3)
        self.assertLess(active_reads[-1], names.index("fleet-deploy"))
        # The previous-invocation read comes after the wait, so the run that
        # was in flight becomes "previous" and the trigger's own run is new.
        invocation_reads = [
            index for index, event in enumerate(self.world.events())
            if event[0] == "ssh" and "--property=InvocationID --value" in str(event[2])
        ]
        self.assertEqual(len(invocation_reads), 1)
        self.assertGreater(invocation_reads[0], active_reads[-1])
        self.assertIn("doc2 activated nixosconfig", proc.stdout)

    def test_in_flight_upgrade_that_never_finishes_times_out(self) -> None:
        self.world.update_state(previous_active_states=["active"])

        proc = self.world.run(extra_env={"CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS": "1"})

        self.assertEqual(proc.returncode, 1)
        self.assertIn("in-flight nixos-upgrade on doc2 to finish", proc.stderr)
        self.assertNotIn("fleet-deploy", self._event_names())

    def test_non_github_flake_input_is_refused_before_any_update(self) -> None:
        self.world.write_lock(self.world.old_target, input_type="git")
        base = self.world.commit_forgejo_master("fixture: git-typed input")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("not a github flake input", proc.stderr)
        self.assertNotIn("nix", self._event_names())
        self.assertNotIn("forgejo-auth", self._event_names())
        self.assertEqual(self.world.forgejo_master(), base)
        self.assertEqual(len(self.world.nixosconfig_worktrees()), 1)

    def test_update_that_pins_the_wrong_revision_is_refused(self) -> None:
        self.world.update_state(fault="nix_wrong_rev")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("does not pin", proc.stderr)
        self.assertNotIn("forgejo-auth", self._event_names())
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)

    def test_stale_readback_after_push_is_a_failed_deploy(self) -> None:
        """The push landed but Forgejo answered with another master: no
        trigger, and the retry simply finds the pin already there."""
        self.world.update_state(fault="readback_stale")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("Forgejo master is " + "d" * 40, proc.stderr)
        self.assertNotIn("fleet-deploy", self._event_names())
        self.assertEqual(self.world.forgejo_master_pins(), self.world.main_tip)

    def test_wrong_host_refuses_before_touching_anything(self) -> None:
        self.world.update_state(hostname="doc2")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 2)
        self.assertIn("run this on doc1", proc.stderr)
        self.assertEqual(self._event_names(), ["hostname"])
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)

    def test_missing_token_refuses_before_fetching(self) -> None:
        self.world.token_file.unlink()

        proc = self.world.run()

        self.assertEqual(proc.returncode, 2)
        self.assertIn("Forgejo token not readable", proc.stderr)
        self.assertEqual(self._event_names(), ["hostname"])

    def test_nix_failure_leaves_master_and_cleans_the_worktree(self) -> None:
        self.world.update_state(fault="nix")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("fake update failure", proc.stderr)
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)
        self.assertNotIn("forgejo-auth", self._event_names())
        self.assertNotIn("fleet-deploy", self._event_names())
        self.assertEqual(len(self.world.nixosconfig_worktrees()), 1)

    def test_nix_debris_outside_the_lock_is_refused(self) -> None:
        self.world.update_state(fault="nix_extra_file")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("something other than flake.lock", proc.stderr)
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)
        self.assertNotIn("forgejo-auth", self._event_names())

    def test_unsigned_commit_is_never_pushed(self) -> None:
        self.world.git(self.world.nixosconfig, "config", "commit.gpgsign", "false")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("not verifiably SSH-signed", proc.stderr)
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)
        self.assertNotIn("forgejo-auth", self._event_names())
        self.assertNotIn("fleet-deploy", self._event_names())

    def test_push_rejection_fails_without_a_trigger(self) -> None:
        self.world.update_state(fault="push")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("fake push rejection", proc.stderr)
        self.assertEqual(self.world.forgejo_master(), self.world.nixosconfig_base)
        self.assertNotIn("fleet-deploy", self._event_names())
        self.assertEqual(len(self.world.nixosconfig_worktrees()), 1)

    def test_fleet_deploy_failure_stops_the_deploy(self) -> None:
        self.world.update_state(fault="fleet_deploy")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("fleet-deploy doc2 failed", proc.stderr)
        # The pin already landed: the retry path is simply to run again.
        self.assertEqual(self.world.forgejo_master_pins(), self.world.main_tip)

    def test_unreachable_host_fails_closed(self) -> None:
        self.world.update_state(fault="ssh")

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("could not read nixos-upgrade state on doc2", proc.stderr)
        self.assertNotIn("fleet-deploy", self._event_names())

    def test_failed_upgrade_prints_the_journal(self) -> None:
        self.world.update_state(upgrade_states=[
            upgrade_state(PREVIOUS_INVOCATION, "inactive", "dead"),
            upgrade_state(NEW_INVOCATION, "activating", "start"),
            upgrade_state(NEW_INVOCATION, "failed", "failed", "exit-code"),
        ])

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("nixos-upgrade failed", proc.stderr)
        self.assertIn("fake nixos-upgrade journal", proc.stderr)
        self.assertNotIn("activated", proc.stdout)

    def test_invocation_that_never_appears_times_out_with_the_journal(self) -> None:
        self.world.update_state(upgrade_states=[
            upgrade_state(PREVIOUS_INVOCATION, "inactive", "dead"),
        ])

        proc = self.world.run(extra_env={"CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS": "1"})

        self.assertEqual(proc.returncode, 1)
        self.assertIn("timed out after 1s", proc.stderr)
        self.assertIn("fake nixos-upgrade journal", proc.stderr)

    def test_invocation_replaced_mid_deploy_is_a_failure(self) -> None:
        self.world.update_state(upgrade_states=[
            upgrade_state(NEW_INVOCATION, "active", "running"),
            upgrade_state("c" * 32, "inactive", "dead"),
        ])

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn("invocation changed mid-deploy", proc.stderr)

    def test_anchor_mismatch_is_a_failed_deploy(self) -> None:
        self.world.update_state(anchor="c" * 40)

        proc = self.world.run()

        self.assertEqual(proc.returncode, 1)
        self.assertIn(f"doc2 activated nixosconfig {'c' * 40}, not", proc.stderr)
        self.assertNotIn("check your change", proc.stdout)


if __name__ == "__main__":
    unittest.main()
