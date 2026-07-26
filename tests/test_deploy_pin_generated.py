"""Generated lifecycle patrol for the nixosconfig deploy-pin entrypoint."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from tests.fakes.deploy_pin import FakeDeployPinCommands


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "pin_nixosconfig.sh"


def assert_deploy_lifecycle_invariants(
    state: dict[str, Any], *, target: str
) -> None:
    """Assert retry safety, ordering, ownership, and cleanup invariants."""
    events = state["events"]
    commits = [event[1] for event in events if event[0] == "commit"]
    receipt_updates = [
        event[2] for event in events
        if event[0] == "update-ref"
        and event[1] == "refs/cratedigger-deploy/cratedigger-src"
    ]
    pushes = [event for event in events if event[0] == "push"]

    if state["receipt_rev"] is not None:
        assert state["receipt_rev"] in state["commits"]
        assert state["commits"][state["receipt_rev"]]["target"] == target
    pending_revision = state.get("pending_rev")
    if pending_revision is not None:
        assert pending_revision in state["commits"]
        definitive_invalid = {
            event[1] for event in events
            if event[0] == "signature-status" and event[2] in {"B", "N"}
        }
        assert pending_revision not in definitive_invalid
    signed_commits = [
        revision for revision in commits
        if state["commits"][revision].get("signature_material", "good")
        == "good"
    ]
    assert len(set(commits)) == len(commits)
    assert len(signed_commits) <= 1
    assert len(receipt_updates) <= 1
    for push in pushes:
        revision = push[1]
        assert revision in receipt_updates
        assert receipt_updates.index(revision) < events.index(push)
        assert push[2] == "header-present"
    worktree_adds = sum(event[0] == "worktree-add" for event in events)
    cleanup_attempts = sum(event[0] == "worktree-remove" for event in events)
    assert cleanup_attempts == worktree_adds


def assert_divergent_receipt_invariants(
    state: dict[str, Any], *, receipt: str, remote: str, new_target: str,
    remote_matches_receipt: bool, verifier_available: bool,
) -> None:
    """A sibling receipt advances only through an equivalent trusted remote."""
    commits = [event for event in state["events"] if event[0] == "commit"]
    receipt_updates = [
        event for event in state["events"]
        if event[:2] == ["update-ref", "refs/cratedigger-deploy/cratedigger-src"]
    ]
    pushes = [event for event in state["events"] if event[0] == "push"]
    if remote_matches_receipt and verifier_available:
        assert len(commits) == 1
        revision = commits[0][1]
        assert state["commits"][revision]["parent"] == remote
        assert state["commits"][revision]["target"] == new_target
        assert state["receipt_rev"] == revision
        assert len(receipt_updates) == 1
        assert len(pushes) == 1
    else:
        assert state["receipt_rev"] == receipt
        assert not commits
        assert not receipt_updates
        assert not pushes


class TestDeployLifecycleCheckerKnownBad(unittest.TestCase):
    def test_divergent_checker_rejects_a_pin_after_untrusted_remote(self) -> None:
        bad = {
            "events": [
                ["commit", "new"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "new"],
                ["push", "new", "header-present"],
            ],
            "commits": {"new": {"parent": "remote", "target": "new-target"}},
            "receipt_rev": "new",
        }
        with self.assertRaises(AssertionError):
            assert_divergent_receipt_invariants(
                bad,
                receipt="receipt",
                remote="remote",
                new_target="new-target",
                remote_matches_receipt=False,
                verifier_available=True,
            )

    def test_checker_rejects_second_pin_commit(self) -> None:
        bad = {
            "events": [
                ["commit", "a"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "a"],
                ["commit", "b"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "b"],
            ],
            "commits": {
                "a": {"target": "t", "signature_material": "good"},
                "b": {"target": "t", "signature_material": "good"},
            },
            "receipt_rev": "b",
        }
        with self.assertRaises(AssertionError):
            assert_deploy_lifecycle_invariants(bad, target="t")

    def test_checker_rejects_two_signed_commits_with_one_receipt(self) -> None:
        bad = {
            "events": [
                ["commit", "a"],
                ["commit", "b"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "b"],
            ],
            "commits": {
                "a": {"target": "t", "signature_material": "good"},
                "b": {"target": "t", "signature_material": "good"},
            },
            "receipt_rev": "b",
        }
        with self.assertRaises(AssertionError):
            assert_deploy_lifecycle_invariants(bad, target="t")

    def test_checker_rejects_persistently_invalid_pending_commit(self) -> None:
        bad = {
            "events": [["commit", "a"], ["signature-status", "a", "B"]],
            "commits": {
                "a": {"target": "t", "signature_material": "bad"},
            },
            "receipt_rev": None,
            "pending_rev": "a",
        }
        with self.assertRaises(AssertionError):
            assert_deploy_lifecycle_invariants(bad, target="t")

    def test_checker_rejects_push_before_durable_receipt(self) -> None:
        bad = {
            "events": [
                ["push", "a", "header-present"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "a"],
            ],
            "commits": {"a": {"target": "t"}},
            "receipt_rev": "a",
        }
        with self.assertRaises(AssertionError):
            assert_deploy_lifecycle_invariants(bad, target="t")

    def test_checker_rejects_missing_cleanup_attempt(self) -> None:
        bad = {
            "events": [["worktree-add", "/tmp/w"]],
            "commits": {},
            "receipt_rev": None,
        }
        with self.assertRaises(AssertionError):
            assert_deploy_lifecycle_invariants(bad, target="t")


class TestGeneratedDeployPinLifecycle(unittest.TestCase):
    @settings(max_examples=30, deadline=None)
    @given(
        first_fault=st.sampled_from(
            (
                None,
                "nix",
                "signature",
                "post_commit_rev_parse",
                "post_commit_verify",
                "post_commit_update_ref",
                "signal_after_commit",
                "invalid_signature_signal_after_commit",
                "push",
                "cleanup",
            )
        ),
        remote_after_failure=st.sampled_from(
            ("unchanged", "pending", "descendant", "other")
        ),
        recovery_verifier=st.sampled_from(("available", "unknown")),
    )
    @example(
        first_fault="push",
        remote_after_failure="unchanged",
        recovery_verifier="available",
    )
    @example(
        first_fault="post_commit_rev_parse",
        remote_after_failure="unchanged",
        recovery_verifier="available",
    )
    @example(
        first_fault="signal_after_commit",
        remote_after_failure="unchanged",
        recovery_verifier="unknown",
    )
    @example(
        first_fault="cleanup",
        remote_after_failure="pending",
        recovery_verifier="available",
    )
    @example(
        first_fault="cleanup",
        remote_after_failure="descendant",
        recovery_verifier="available",
    )
    def test_retry_never_silently_creates_a_second_signed_pin(
        self,
        first_fault: str | None,
        remote_after_failure: str,
        recovery_verifier: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            fake = FakeDeployPinCommands(Path(tempdir))
            fake.update_state(fault=first_fault)
            fake.run(SCRIPT)
            after_first = fake.state
            pending = after_first["receipt_rev"]

            if pending is not None and remote_after_failure != "unchanged":
                if remote_after_failure == "pending":
                    target = after_first["commits"][pending]["target"]
                    fake.update_state(remote_rev=pending, remote_target=target)
                elif remote_after_failure == "descendant":
                    target = after_first["commits"][pending]["target"]
                    fake.update_state(
                        remote_rev=fake.OTHER_REV,
                        remote_target=target,
                        remote_ancestors=[pending],
                    )
                else:
                    fake.update_state(
                        remote_rev=fake.OTHER_REV,
                        remote_target=fake.OLD_TARGET,
                    )
            pending_before_retry = fake.state["pending_rev"]
            commits_before_retry = fake.state["commit_count"]
            if recovery_verifier == "unknown":
                fake.update_state(fault="signature_unknown")
            else:
                fake.clear_fault()
            fake.run(SCRIPT)
            if (
                pending_before_retry is not None
                and recovery_verifier == "unknown"
            ):
                self.assertEqual(fake.state["pending_rev"], pending_before_retry)
                self.assertEqual(fake.state["commit_count"], commits_before_retry)
            assert_deploy_lifecycle_invariants(
                fake.state, target=fake.TARGET_REV
            )

    @settings(max_examples=12, deadline=None)
    @given(
        remote_matches_receipt=st.booleans(),
        verifier_available=st.booleans(),
    )
    @example(remote_matches_receipt=True, verifier_available=True)
    @example(remote_matches_receipt=True, verifier_available=False)
    @example(remote_matches_receipt=False, verifier_available=True)
    @example(remote_matches_receipt=False, verifier_available=False)
    def test_divergent_sibling_receipt_is_superseded_only_by_equivalent_verified_remote(
        self, remote_matches_receipt: bool, verifier_available: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            fake = FakeDeployPinCommands(Path(tempdir))
            receipt = fake.seed_divergent_receipt(
                remote_target=(
                    fake.OLD_TARGET if remote_matches_receipt else fake.TARGET_REV
                )
            )
            fake.update_state(
                remote_signature_status="G" if verifier_available else "U"
            )

            fake.run(SCRIPT)

            assert_divergent_receipt_invariants(
                fake.state,
                receipt=receipt,
                remote=fake.OTHER_REV,
                new_target=fake.TARGET_REV,
                remote_matches_receipt=remote_matches_receipt,
                verifier_available=verifier_available,
            )


if __name__ == "__main__":
    unittest.main()
