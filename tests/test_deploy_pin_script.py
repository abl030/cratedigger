"""Deterministic contract pins for scripts/pin_nixosconfig.sh."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from tests._source_pins import pinned_source
from tests.fakes.deploy_pin import FakeDeployPinCommands
from tests.fakes.subprocess_env import BYTECODE_CACHE_OPT_OUT_VARS
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
        source = pinned_source(SKILL)
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
        self.assertIn(
            ["nix", "flake", "update", "cratedigger-src", "--override-input",
             "cratedigger-src",
             f"github:abl030/cratedigger/{self.fake.TARGET_REV}"],
            state["events"],
        )
        self.assertIn(["ls-remote"], state["events"])
        self.assertIsNone(state["worktree"])
        self.assertIn("signed nixosconfig revision", proc.stdout)
        for call in state["argv_calls"]:
            self.assertNotIn("test-secret-token", " ".join(call))
            self.assertNotIn("Authorization:", " ".join(call))

    def test_no_newline_token_fixture_matches_production_and_succeeds(self) -> None:
        token_bytes = self.fake.token_file.read_bytes()
        self.assertEqual(token_bytes, self.fake.TOKEN_BYTES)
        self.assertEqual(len(token_bytes), 40)
        self.assertFalse(token_bytes.endswith(b"\n"))

        proc = self.fake.run(SCRIPT)

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(self.fake.state["remote_rev"],
                         self.fake.state["receipt_rev"])

    def test_empty_token_is_rejected_before_authenticated_git(self) -> None:
        self.fake.token_file.write_bytes(b"")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("Forgejo token is empty", proc.stderr)
        self.assertFalse(any(event[0] in {"push", "ls-remote"}
                             for event in state["events"]))

    def test_inherited_xtrace_never_prints_token(self) -> None:
        proc = self.fake.run(
            SCRIPT,
            extra_env={"SHELLOPTS": "braceexpand:hashall:xtrace"},
        )
        output = proc.stdout + proc.stderr

        self.assertEqual(proc.returncode, 0, output)
        self.assertNotIn("test-secret-token", output)
        self.assertNotIn("Authorization: token", output)

    def test_inherited_git_trace2_never_prints_token(self) -> None:
        proc = self.fake.run(
            SCRIPT,
            extra_env={
                "GIT_TRACE2": "1",
                "GIT_TRACE2_ENV_VARS": "GIT_CONFIG_VALUE_0",
            },
        )
        output = proc.stdout + proc.stderr

        self.assertEqual(proc.returncode, 0, output)
        self.assertNotIn("test-secret-token", output)
        self.assertNotIn("Authorization: token", output)

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
        self.assertIsNone(state["pending_rev"])
        self.assertFalse(any(event[0] == "push" for event in state["events"]))
        self.assertIsNone(state["worktree"])

    def test_pin_commit_carrying_more_than_the_lock_is_definitively_invalid(
        self,
    ) -> None:
        """#1172 item 2. ``verify_pin_commit`` rejects a pin revision whose
        tree changes anything besides ``flake.lock``, and returns 2 —
        "definitively invalid", which discards the pending candidate rather
        than leaving it to be recovered on the next run.

        Until the fake's ``diff-tree`` consulted the revision it was asked
        about, it answered ``flake.lock`` for every commit, so this guard was
        unreachable from all 47 tests in this file and its generated sibling.
        A correctly signed commit that smuggled a module change in alongside
        the lock bump would have been pinned and deployed.
        """
        self.fake.update_state(fault="extra_changed_paths")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 2, proc.stderr)
        self.assertIn("changes paths other than flake.lock", proc.stderr)
        self.assertIn("modules/nixos/services/cratedigger.nix", proc.stderr)
        # Definitively invalid: not left pending, never pushed, never a receipt.
        self.assertIsNone(state["receipt_rev"])
        self.assertFalse(any(event[0] == "push" for event in state["events"]))

    def test_post_commit_failures_recover_one_exact_signed_commit(self) -> None:
        for fault in (
            "post_commit_rev_parse",
            "post_commit_verify",
            "post_commit_update_ref",
        ):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                fake.update_state(fault=fault)
                first = fake.run(SCRIPT)
                pending = fake.state["pending_rev"]

                self.assertNotEqual(first.returncode, 0)
                self.assertIsNotNone(pending)
                self.assertIn(pending, fake.state["commits"])
                self.assertIn(str(pending), first.stderr)

                fake.clear_fault()
                second = fake.run(SCRIPT)
                state = fake.state
                self.assertEqual(second.returncode, 0, second.stderr)
                self.assertEqual(state["commit_count"], 1)
                self.assertEqual(state["receipt_rev"], pending)
                self.assertIsNone(state["pending_rev"])

    def test_signal_after_commit_preserves_and_recovers_exact_commit(self) -> None:
        self.fake.update_state(fault="signal_after_commit")
        first = self.fake.run(SCRIPT)
        pending = self.fake.state["pending_rev"]

        self.assertNotEqual(first.returncode, 0)
        self.assertIsNotNone(pending)
        self.assertIn(pending, self.fake.state["commits"])

        self.fake.update_state(fault="post_commit_verify")
        transient_failure = self.fake.run(SCRIPT)
        self.assertNotEqual(transient_failure.returncode, 0)
        self.assertEqual(self.fake.state["pending_rev"], pending)
        self.assertEqual(self.fake.state["commit_count"], 1)

        self.fake.clear_fault()
        second = self.fake.run(SCRIPT)
        state = self.fake.state
        self.assertEqual(second.returncode, 0, second.stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["receipt_rev"], pending)
        self.assertIsNone(state["pending_rev"])

    def test_guarded_kill_reaches_the_real_shell_parent(self) -> None:
        """Issue #1250 review finding F2 (and R1's follow-up correction):
        every fault-injection assertion above (and elsewhere in this
        file) only checks `returncode != 0`, which the fake git shim's
        own unconditional `raise SystemExit(143)` satisfies whether or
        not the guarded kill inside it ever actually fires --
        production `pin_nixosconfig.sh`'s `set -euo pipefail` plus its
        `trap 'exit 143' TERM` are DESIGNED so a real async SIGTERM and
        an ordinary same-numbered command failure converge on the
        identical observable exit code, so none of the other tests in
        this module constrain the three guarded kills in
        `tests/fakes/deploy_pin.py`'s shim at all.

        This drives the SAME fake git stub `deploy_pin.py` ships (the
        real `_SHIM_MODULE` source, same guard) directly, against a
        minimal, fully-controlled bash parent that installs its own TERM
        trap writing a marker file. Bash's own documented deferred-trap
        behaviour -- a pending trap only runs once the CURRENTLY
        EXECUTING foreground command completes -- makes "did the trap
        fire before the no-signal fallthrough line" a clean, non-racy
        proof that the kernel actually delivered a real SIGTERM to this
        exact parent PID: with the guard refusing, bash never receives
        anything asynchronous at all and simply continues past the child
        to the fallthrough `printf` line once it exits; with the guard
        correctly firing, the trap always wins the race. Method and
        measured trial counts:
        `docs/solutions/testing/parent-signal-guard-worker-death-fixture.md`.

        R1: a subTest per ARGV SHAPE, one for each of the three guarded
        kill call sites in `deploy_pin.py` (`rev-parse --verify --quiet`
        against the receipt ref, `rev-parse --verify` against the
        pending ref, and `rev-parse HEAD`) -- a single case exercising
        only the first left the other two completely unconstrained.
        """
        commit_stub = {
            "parent": "0" * 40,
            "target": "1" * 40,
            "message": "test",
            "signature_material": "good",
            "changed_paths": ["flake.lock"],
        }
        cases = (
            (
                "rev-parse --verify --quiet (receipt ref)",
                {
                    "fault": "signal_after_commit",
                    "receipt_rev": "a" * 40,
                    "pending_rev": None,
                    "commits": {"a" * 40: commit_stub},
                },
                [
                    "rev-parse", "--verify", "--quiet",
                    "refs/cratedigger-deploy/cratedigger-src",
                ],
            ),
            (
                "rev-parse --verify (pending ref, non-quiet)",
                {
                    "fault": "signal_after_pending_commit",
                    "receipt_rev": None,
                    "pending_rev": "b" * 40,
                    "commits": {"b" * 40: commit_stub},
                },
                [
                    "rev-parse", "--verify",
                    "refs/cratedigger-deploy/cratedigger-src-pending",
                ],
            ),
            (
                "rev-parse HEAD",
                {
                    "fault": "signal_after_commit",
                    "receipt_rev": None,
                    "pending_rev": None,
                    "commits": {},
                },
                ["rev-parse", "HEAD"],
            ),
        )
        git_stub = self.fake.fake_bin / "git"

        for index, (description, state_changes, argv) in enumerate(cases):
            with self.subTest(site=description):
                self.fake.update_state(**state_changes)
                marker = Path(self.tempdir.name) / f"sigterm-received-{index}"
                wrapper_source = (
                    f"trap 'printf caught > {marker} ; exit 143' TERM\n"
                    '"$@"\n'
                    'printf "no-signal rc=$?\\n"\n'
                )

                proc = subprocess.run(
                    ["bash", "-c", wrapper_source, "wrapper",
                     str(git_stub), *argv],
                    env=self.fake.environment(self.fake.TARGET_REV),
                    capture_output=True,
                    text=True,
                    timeout=15,
                    check=False,
                )

                self.assertEqual(
                    proc.returncode, 143, proc.stdout + proc.stderr,
                )
                self.assertEqual(proc.stdout, "")
                self.assertTrue(
                    marker.exists(),
                    "the guarded kill in tests/fakes/deploy_pin.py never "
                    "delivered a real SIGTERM to this wrapper's own "
                    f"parent PID for {description!r} -- wrapper "
                    f"stdout={proc.stdout!r}",
                )

    def test_recovery_discards_persistently_invalid_pending_commit(self) -> None:
        self.fake.update_state(fault="invalid_signature_signal_after_commit")
        interrupted = self.fake.run(SCRIPT)
        invalid = self.fake.state["pending_rev"]

        self.assertNotEqual(interrupted.returncode, 0)
        self.assertIsNotNone(invalid)
        self.assertEqual(
            self.fake.state["commits"][invalid]["signature_material"], "bad"
        )

        self.fake.clear_fault()
        rejected = self.fake.run(SCRIPT)
        state_after_rejection = self.fake.state
        self.assertEqual(rejected.returncode, 2, rejected.stderr)
        self.assertIn("definitively invalid pending candidate", rejected.stderr)
        self.assertIsNone(state_after_rejection["pending_rev"])
        self.assertIsNone(state_after_rejection["receipt_rev"])
        self.assertEqual(state_after_rejection["commit_count"], 1)

        recovered = self.fake.run(SCRIPT)
        final_state = self.fake.state
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(final_state["commit_count"], 2)
        self.assertEqual(
            final_state["commits"][final_state["receipt_rev"]][
                "signature_material"
            ],
            "good",
        )
        self.assertIsNone(final_state["pending_rev"])

    def test_unknown_signature_verifier_retains_same_pending_commit(self) -> None:
        self.fake.update_state(fault="signal_after_commit")
        interrupted = self.fake.run(SCRIPT)
        pending = self.fake.state["pending_rev"]

        self.assertNotEqual(interrupted.returncode, 0)
        self.assertIsNotNone(pending)
        self.assertEqual(
            self.fake.state["commits"][pending]["signature_material"], "good"
        )

        self.fake.update_state(fault="signature_unknown")
        unavailable = self.fake.run(SCRIPT)
        self.assertEqual(unavailable.returncode, 1, unavailable.stderr)
        self.assertEqual(self.fake.state["pending_rev"], pending)
        self.assertEqual(self.fake.state["commit_count"], 1)

        self.fake.clear_fault()
        recovered = self.fake.run(SCRIPT)
        final_state = self.fake.state
        self.assertEqual(recovered.returncode, 0, recovered.stderr)
        self.assertEqual(final_state["commit_count"], 1)
        self.assertEqual(final_state["receipt_rev"], pending)
        self.assertIsNone(final_state["pending_rev"])

    def test_pending_recovery_leaves_private_refs_untouched_when_remote_is_untrusted(
        self,
    ) -> None:
        cases = (
            ("bad signature", {"remote_signature_status": "B"}),
            ("unknown signature", {"remote_signature_status": "U"}),
            ("unreadable lock", {"remote_lock_readable": False}),
            ("wrong target", {"remote_target": "7" * 40}),
        )
        for name, changes in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                receipt = fake.seed_divergent_receipt()
                fake.update_state(
                    fault="signal_after_pending_commit", remote_move_on_nix=True
                )
                interrupted = fake.run(SCRIPT)
                pending = fake.state["pending_rev"]
                self.assertNotEqual(interrupted.returncode, 0)
                self.assertIsNotNone(pending)

                event_count = len(fake.state["events"])
                fake.update_state(
                    fault=None, remote_move_on_nix=False, **changes
                )
                retry = fake.run(SCRIPT)
                state = fake.state

                self.assertNotEqual(retry.returncode, 0)
                self.assertEqual(state["receipt_rev"], receipt)
                self.assertEqual(state["pending_rev"], pending)
                self.assertEqual(state["commit_count"], 1)
                private_ref_mutations = [
                    event for event in state["events"][event_count:]
                    if event[0] in {"update-ref", "delete-ref"}
                    and event[1] in {
                        "refs/cratedigger-deploy/cratedigger-src",
                        "refs/cratedigger-deploy/cratedigger-src-pending",
                    }
                ]
                self.assertEqual(private_ref_mutations, [])

    def test_pending_recovery_rebuilds_from_trusted_divergent_parent_target(
        self,
    ) -> None:
        receipt = self.fake.seed_divergent_receipt()
        self.fake.update_state(
            fault="signal_after_pending_commit", remote_move_on_nix=True
        )
        interrupted = self.fake.run(SCRIPT)
        pending = self.fake.state["pending_rev"]

        self.assertNotEqual(interrupted.returncode, 0)
        self.assertIsNotNone(pending)

        self.fake.update_state(fault=None, remote_move_on_nix=False)
        retry = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(state["commit_count"], 2)
        self.assertEqual(state["receipt_rev"], state["remote_rev"])
        self.assertNotEqual(state["receipt_rev"], pending)
        self.assertEqual(
            state["commits"][state["receipt_rev"]]["parent"], "6" * 40
        )
        self.assertNotEqual(state["receipt_rev"], receipt)

    def test_pending_recovery_accepts_trusted_remote_at_candidate(self) -> None:
        self.fake.update_state(fault="signal_after_commit")
        interrupted = self.fake.run(SCRIPT)
        pending = self.fake.state["pending_rev"]

        self.assertNotEqual(interrupted.returncode, 0)
        self.assertIsNotNone(pending)

        self.fake.update_state(
            fault=None,
            remote_rev=pending,
            remote_target=self.fake.TARGET_REV,
        )
        retry = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(retry.returncode, 0, retry.stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["receipt_rev"], pending)
        self.assertEqual(state["remote_rev"], pending)
        self.assertIsNone(state["pending_rev"])

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

        incompatible_target = "7" * 40
        self.fake.update_state(fault=None, remote_rev=self.fake.OTHER_REV,
                               remote_target=incompatible_target)
        second = self.fake.run(SCRIPT)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn(f"pending={pending}", second.stderr)
        self.assertIn(f"base={parent}", second.stderr)
        self.assertIn(f"remote={self.fake.OTHER_REV}", second.stderr)
        self.assertEqual(self.fake.state["commit_count"], 1)

    def test_equivalent_sibling_receipt_allows_the_next_target(self) -> None:
        receipt = self.fake.seed_divergent_receipt()

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["commit_count"], 1)
        self.assertEqual(state["remote_target"], self.fake.TARGET_REV)
        self.assertEqual(state["receipt_rev"], state["remote_rev"])
        self.assertNotEqual(state["receipt_rev"], receipt)
        self.assertEqual(
            state["commits"][state["receipt_rev"]]["parent"],
            self.fake.OTHER_REV,
        )
        self.assertEqual(
            sum(event[0] == "push" for event in state["events"]), 1
        )

    def test_equivalent_sibling_same_target_reports_current_signed_remote(
        self,
    ) -> None:
        receipt = self.fake.seed_divergent_receipt(
            receipt_target=self.fake.TARGET_REV,
            remote_target=self.fake.TARGET_REV,
        )

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["receipt_rev"], receipt)
        self.assertEqual(state["commit_count"], 0)
        self.assertFalse(any(event[0] == "push" for event in state["events"]))
        self.assertIn("remote preserves pending target", proc.stdout)

    def test_retry_replaces_candidate_rejected_after_equivalence_check(self) -> None:
        for move_hook in (
            "remote_move_on_worktree_add",
            "remote_move_on_nix",
            "remote_move_on_push",
        ):
            with self.subTest(move_hook=move_hook), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                stale_receipt = fake.seed_divergent_receipt()
                fake.update_state(**{move_hook: True})

                first = fake.run(SCRIPT)
                rejected_candidate = fake.state["receipt_rev"]

                self.assertNotEqual(first.returncode, 0)
                self.assertNotEqual(rejected_candidate, stale_receipt)
                self.assertEqual(fake.state["remote_rev"], "6" * 40)
                self.assertEqual(fake.state["commit_count"], 1)
                self.assertEqual(
                    fake.state["commits"][rejected_candidate]["parent"],
                    fake.OTHER_REV,
                )

                fake.update_state(**{move_hook: False})
                retry = fake.run(SCRIPT)
                state = fake.state

                self.assertEqual(retry.returncode, 0, retry.stderr)
                self.assertEqual(state["commit_count"], 2)
                self.assertEqual(state["receipt_rev"], state["remote_rev"])
                self.assertNotEqual(state["receipt_rev"], rejected_candidate)
                self.assertEqual(
                    state["commits"][state["receipt_rev"]]["parent"], "6" * 40
                )
                receipt_updates = [
                    event for event in state["events"]
                    if event[:2] == [
                        "update-ref", "refs/cratedigger-deploy/cratedigger-src"
                    ]
                ]
                self.assertEqual(receipt_updates[-1][3], rejected_candidate)

    def test_override_input_pins_exact_target_when_branch_tip_differs(
        self,
    ) -> None:
        """#1203 item 1. Production pins the exact requested revision via
        ``nix flake update --override-input``, not the branch tip: setting
        ``branch_tip`` to a revision distinct from the requested target and
        still landing that exact target proves the override -- not a tip
        coincidence -- did the pinning. Kills a mutant that reverts to plain
        ``nix flake update cratedigger-src``: the fake's plain-form branch
        pins ``branch_tip`` instead, which does not equal the requested
        target and trips the existing post-update guard.
        """
        self.fake.update_state(branch_tip="9" * 40)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["remote_target"], self.fake.TARGET_REV)
        self.assertIn(
            [
                "nix", "flake", "update", "cratedigger-src",
                "--override-input", "cratedigger-src",
                f"github:abl030/cratedigger/{self.fake.TARGET_REV}",
            ],
            state["events"],
        )

    def test_two_step_recovery_lands_abandoned_receipt_then_the_intended_target(
        self,
    ) -> None:
        """#1203 item 1, the exact deadlock. A prior session signed but never
        pushed a receipt targeting an old revision; Forgejo master has since
        advanced independently (still pinning the receipt's own parent
        target), and the branch tip has moved to a THIRD revision, so the
        old target is no longer the tip either. Before this fix, requesting
        the new target hit ``different pin is still pending`` and requesting
        the old target hit ``updated flake.lock does not pin requested
        Cratedigger revision`` -- no sanctioned exit. The two-step recovery:
        re-run with the receipt's own target first (lands it, since the
        helper now pins exactly what is asked regardless of the tip), then
        re-run with the originally intended target (proceeds normally, since
        the receipt is now master).
        """
        abandoned_target = "8" * 40
        intended_target = self.fake.TARGET_REV
        receipt = self.fake.seed_divergent_receipt(receipt_target=abandoned_target)
        self.fake.update_state(branch_tip="9" * 40)

        step1 = self.fake.run(SCRIPT, target=abandoned_target)
        state_after_step1 = self.fake.state
        self.assertEqual(step1.returncode, 0, step1.stderr)
        self.assertEqual(state_after_step1["remote_target"], abandoned_target)
        self.assertEqual(
            state_after_step1["receipt_rev"], state_after_step1["remote_rev"]
        )
        self.assertNotEqual(state_after_step1["receipt_rev"], receipt)
        self.assertEqual(state_after_step1["commit_count"], 1)

        step2 = self.fake.run(SCRIPT, target=intended_target)
        state = self.fake.state
        self.assertEqual(step2.returncode, 0, step2.stderr)
        self.assertEqual(state["remote_target"], intended_target)
        self.assertEqual(state["receipt_rev"], state["remote_rev"])
        self.assertEqual(state["commit_count"], 2)
        self.assertEqual(
            sum(event[0] == "push" for event in state["events"]), 2
        )

    def test_divergent_receipt_failure_names_the_two_step_recovery(self) -> None:
        """#1203 item 1. The surviving deadlock (requesting a new target
        while an old, un-landed receipt diverges from an untrusted remote)
        must name its own exit: re-running with the receipt's own pinned
        target lands it first, after which the original request can be
        retried. Every existing coordinate stays in the message.
        """
        receipt = self.fake.seed_divergent_receipt()
        self.fake.update_state(remote_target="7" * 40)

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["receipt_rev"], receipt)
        self.assertIn(f"requested={self.fake.TARGET_REV}", proc.stderr)
        self.assertIn(f"pending_target={self.fake.OLD_TARGET}", proc.stderr)
        self.assertIn(f"pending={receipt}", proc.stderr)
        self.assertIn(f"base={self.fake.BASE_REV}", proc.stderr)
        self.assertIn(f"remote={self.fake.OTHER_REV}", proc.stderr)
        self.assertIn(
            f"re-run with target={self.fake.OLD_TARGET} first to land it",
            proc.stderr,
        )
        self.assertIn(
            f"then re-run with target={self.fake.TARGET_REV}", proc.stderr
        )

    def test_nonexistent_requested_revision_fails_closed(self) -> None:
        """Models the real failure of asking to override to a revision that
        does not exist on GitHub: ``nix flake update --override-input``
        exits nonzero, same fail-closed shape as any other pre-commit nix
        failure -- no receipt, no push, no worktree left behind."""
        self.fake.update_state(fault="nix_missing_revision")

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(state["remote_rev"], self.fake.BASE_REV)
        self.assertIsNone(state["receipt_rev"])
        self.assertIsNone(state["worktree"])
        self.assertFalse(any(event[0] == "push" for event in state["events"]))

    def test_override_ref_is_derived_from_flake_lock_original_not_hardcoded(
        self,
    ) -> None:
        """The overridable flake ref is read from flake.lock's own
        cratedigger-src ``original`` node, never a second hardcoded copy of
        ``github:abl030/cratedigger`` -- a non-default owner/repo here must
        show up verbatim in the ``nix`` argv."""
        self.fake.update_state(cratedigger_input_original={
            "type": "github", "owner": "example-org", "repo": "cratedigger-fork",
        })

        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertEqual(proc.returncode, 0, proc.stderr)
        self.assertEqual(state["remote_target"], self.fake.TARGET_REV)
        self.assertIn(
            [
                "nix", "flake", "update", "cratedigger-src",
                "--override-input", "cratedigger-src",
                f"github:example-org/cratedigger-fork/{self.fake.TARGET_REV}",
            ],
            state["events"],
        )

    def test_malformed_flake_lock_original_fails_closed(self) -> None:
        cases = (
            (
                "non-github type",
                {"type": "indirect", "owner": "abl030", "repo": "cratedigger"},
                "must be github",
            ),
            (
                "missing owner",
                {"type": "github", "repo": "cratedigger"},
                "owner is missing or malformed",
            ),
            (
                "missing repo",
                {"type": "github", "owner": "abl030"},
                "repo is missing or malformed",
            ),
            (
                "malformed owner",
                {"type": "github", "owner": "abl/030", "repo": "cratedigger"},
                "owner is missing or malformed",
            ),
            (
                # #1203 correction round, finding 3: the docstring promised
                # "fails closed on any input shape this script does not
                # understand" but only type/owner/repo were checked -- a
                # `host` key (redirecting a github-type input at a private
                # mirror) was silently dropped, building a ref that points
                # at github.com regardless of what `host` said.
                "unrecognised key",
                {
                    "type": "github", "owner": "abl030", "repo": "cratedigger",
                    "host": "github.example.com",
                },
                "unrecognised keys",
            ),
        )
        for name, original, message_fragment in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                fake.update_state(cratedigger_input_original=original)

                proc = fake.run(SCRIPT)
                state = fake.state

                self.assertNotEqual(proc.returncode, 0)
                self.assertIn(message_fragment, proc.stderr)
                self.assertIsNone(state["receipt_rev"])
                self.assertIsNone(state["worktree"])
                self.assertFalse(
                    any(event[0] == "push" for event in state["events"])
                )

    def test_divergent_receipt_fails_closed_without_equivalent_verified_remote(
        self,
    ) -> None:
        cases = (
            ("wrong target", {"remote_target": self.fake.TARGET_REV}),
            ("bad signature", {"remote_signature_status": "B"}),
            ("unknown signature", {"remote_signature_status": "U"}),
            ("unreadable lock", {"remote_lock_readable": False}),
            ("remote changed", {"remote_change_on_ls_remote_call": 2}),
        )
        for name, changes in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                receipt = fake.seed_divergent_receipt()
                fake.update_state(**changes)

                proc = fake.run(SCRIPT)
                state = fake.state

                self.assertNotEqual(proc.returncode, 0, proc.stderr)
                self.assertEqual(state["receipt_rev"], receipt)
                self.assertEqual(state["commit_count"], 0)
                self.assertFalse(any(event[0] == "push" for event in state["events"]))
                self.assertFalse(
                    any(
                        event[0] == "update-ref"
                        and event[1] == "refs/cratedigger-deploy/cratedigger-src"
                        for event in state["events"]
                    )
                )

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
            fetch_urls=["https://github.com/abl030/nixosconfig.git"]
        )
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("fetch URL must be https://git.ablz.au", proc.stderr)
        self.assertFalse(any(event[0] == "fetch" for event in state["events"]))
        self.assertFalse(any(event[0] == "ls-remote"
                             for event in state["events"]))

    def test_distinct_push_url_fails_before_token_read_or_push(self) -> None:
        self.fake.token_file.unlink()
        self.fake.update_state(
            push_urls=["https://example.invalid/attacker/nixosconfig.git"]
        )
        proc = self.fake.run(SCRIPT)
        state = self.fake.state

        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("push URL", proc.stderr)
        self.assertNotIn("token", proc.stderr.lower())
        self.assertFalse(any(event[0] == "fetch" for event in state["events"]))
        self.assertFalse(any(event[0] == "push" for event in state["events"]))

    def test_multiple_fetch_or_push_urls_fail_before_token_or_network(self) -> None:
        cases = (
            {
                "fetch_urls": [
                    "https://git.ablz.au/abl030/nixosconfig.git",
                    "https://example.invalid/second.git",
                ],
            },
            {
                "push_urls": [
                    "https://git.ablz.au/abl030/nixosconfig.git",
                    "https://example.invalid/second.git",
                ],
            },
        )
        for changes in cases:
            with self.subTest(changes=changes), tempfile.TemporaryDirectory() as td:
                fake = FakeDeployPinCommands(Path(td))
                fake.token_file.unlink()
                fake.update_state(**changes)
                proc = fake.run(SCRIPT)
                state = fake.state
                self.assertNotEqual(proc.returncode, 0)
                self.assertIn("exactly one", proc.stderr)
                self.assertNotIn("token", proc.stderr.lower())
                self.assertFalse(any(event[0] in {"fetch", "push"}
                                     for event in state["events"]))


class TestDeployPinFakeShimCaching(unittest.TestCase):
    """Pins for the shared-module fake-command shape (issue #1156 item 4):
    each fake command (git/nix/hostname) is a tiny stub importing one shared
    ``_shim.py``, so CPython caches its compiled bytecode across the ~28
    subprocess spawns per script run instead of recompiling the whole body
    on every one."""

    def test_stubs_are_tiny_and_share_one_cached_shim_module(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake = FakeDeployPinCommands(Path(td))
            shim_path = fake.fake_bin / "_shim.py"
            self.assertTrue(shim_path.exists())
            shim_size = shim_path.stat().st_size
            for name in ("git", "nix", "hostname"):
                stub_size = (fake.fake_bin / name).stat().st_size
                # A regression back to writing the full body into every fake
                # command (the pre-#1156-item-4 shape) would make each stub
                # as large as the shim itself.
                self.assertLess(stub_size, 300, f"{name} stub is not tiny")
                self.assertLess(stub_size * 5, shim_size,
                                 f"{name} stub looks like a full shim copy")

            pycache = fake.fake_bin / "__pycache__"
            self.assertFalse(pycache.exists())

            # Run with the opt-outs SET: a mutant runner must export
            # PYTHONDONTWRITEBYTECODE=1, and the fixture's own
            # environment() is what keeps that from reaching the stub
            # (issue #1313, 1329-2).
            with patch.dict(
                os.environ,
                {name: "1" for name in BYTECODE_CACHE_OPT_OUT_VARS},
            ):
                proc = subprocess.run(
                    [str(fake.fake_bin / "hostname")],
                    env=fake.environment(fake.TARGET_REV),
                    capture_output=True,
                    text=True,
                    check=False,
                )
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(proc.stdout.strip(), "proxmox-vm")

            cached = list(pycache.glob("_shim.*.pyc"))
            self.assertEqual(
                len(cached), 1,
                "expected the shim's bytecode to be cached in __pycache__ "
                f"after one call, found {cached} -- the fixture's own "
                "environment() is what must drop PYTHONDONTWRITEBYTECODE "
                "and PYTHONPYCACHEPREFIX, both of which silently defeat "
                "this caching",
            )

    def test_stub_fails_loudly_without_the_shared_shim_module(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            fake = FakeDeployPinCommands(Path(td))
            (fake.fake_bin / "_shim.py").unlink()

            proc = subprocess.run(
                [str(fake.fake_bin / "hostname")],
                env=fake.environment(fake.TARGET_REV),
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("ModuleNotFoundError", proc.stderr)
            self.assertIn("_shim", proc.stderr)


if __name__ == "__main__":
    unittest.main()
