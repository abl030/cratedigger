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
RECEIPT_REF = "refs/cratedigger-deploy/cratedigger-src"
PENDING_REF = "refs/cratedigger-deploy/cratedigger-src-pending"


def assert_deploy_lifecycle_invariants(
    state: dict[str, Any], *, target: str, max_signed_commits: int = 1,
    max_receipt_updates: int = 1,
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
    ref_updates = [event for event in events if event[0] == "update-ref"]
    ref_deletes = [event for event in events if event[0] == "delete-ref"]

    for event in ref_updates:
        assert len(event) == 4
        assert isinstance(event[3], str)
    for event in ref_deletes:
        assert len(event) == 3
        assert isinstance(event[2], str)

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
    assert len(signed_commits) <= max_signed_commits
    assert len(receipt_updates) <= max_receipt_updates
    for push in pushes:
        revision = push[1]
        assert revision in receipt_updates
        assert receipt_updates.index(revision) < events.index(push)
        assert push[2] == "header-present"
    worktree_adds = sum(event[0] == "worktree-add" for event in events)
    cleanup_attempts = sum(event[0] == "worktree-remove" for event in events)
    assert cleanup_attempts == worktree_adds


def assert_divergent_receipt_invariants(
    state: dict, *, receipt: str, remote: str, new_target: str,
    remote_matches_receipt: bool, verifier_available: bool,
) -> None:
    """A sibling receipt advances only through an equivalent trusted remote."""
    commits = [event for event in state["events"] if event[0] == "commit"]
    receipt_updates = [
        event for event in state["events"]
        if event[:2] == ["update-ref", "refs/cratedigger-deploy/cratedigger-src"]
    ]
    pushes = [event for event in state["events"] if event[0] == "push"]
    pending_updates = [
        event for event in state["events"]
        if event[:2] == ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending"]
    ]
    pending_deletes = [
        event for event in state["events"]
        if event[:2] == ["delete-ref", "refs/cratedigger-deploy/cratedigger-src-pending"]
    ]
    if remote_matches_receipt and verifier_available:
        assert len(commits) == 1
        revision = commits[0][1]
        assert state["commits"][revision]["parent"] == remote
        assert state["commits"][revision]["target"] == new_target
        assert state["receipt_rev"] == revision
        assert receipt_updates == [
            ["update-ref", "refs/cratedigger-deploy/cratedigger-src", revision, receipt]
        ]
        assert pending_updates == [
            ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending", remote, ""]
        ]
        assert pending_deletes == [
            ["delete-ref", "refs/cratedigger-deploy/cratedigger-src-pending", revision]
        ]
        assert len(pushes) == 1
    else:
        assert state["receipt_rev"] == receipt
        assert not commits
        assert not receipt_updates
        assert not pending_updates
        assert not pending_deletes
        assert not pushes


def assert_same_target_divergent_invariants(
    state: dict, *, receipt: str, remote: str, target: str,
    remote_relation: str, verifier_available: bool,
) -> None:
    """A same-target sibling is current, replaced, or left untouched."""
    commits = [event for event in state["events"] if event[0] == "commit"]
    receipt_updates = [
        event for event in state["events"]
        if event[:2] == ["update-ref", "refs/cratedigger-deploy/cratedigger-src"]
    ]
    pending_updates = [
        event for event in state["events"]
        if event[:2] == ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending"]
    ]
    pending_deletes = [
        event for event in state["events"]
        if event[:2] == ["delete-ref", "refs/cratedigger-deploy/cratedigger-src-pending"]
    ]
    pushes = [event for event in state["events"] if event[0] == "push"]
    if verifier_available and remote_relation == "parent":
        assert len(commits) == 1
        revision = commits[0][1]
        assert state["commits"][revision]["parent"] == remote
        assert state["commits"][revision]["target"] == target
        assert state["receipt_rev"] == revision
        assert receipt_updates == [
            ["update-ref", "refs/cratedigger-deploy/cratedigger-src", revision, receipt]
        ]
        assert pending_updates == [
            ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending", remote, ""]
        ]
        assert pending_deletes == [
            ["delete-ref", "refs/cratedigger-deploy/cratedigger-src-pending", revision]
        ]
        assert len(pushes) == 1
    else:
        assert state["receipt_rev"] == receipt
        assert not commits
        assert not receipt_updates
        assert not pending_updates
        assert not pending_deletes
        assert not pushes


def assert_pending_recovery_invariants(
    state: dict, *, receipt: str, pending: str, event_start: int,
    commit_count: int, outcome: str,
) -> None:
    """An interrupted pending candidate is fail-closed until its remote is safe."""
    events = state["events"][event_start:]
    private_mutations = [
        event for event in events
        if event[0] in {"update-ref", "delete-ref"}
        and event[1] in {RECEIPT_REF, PENDING_REF}
    ]
    commits = [event for event in events if event[0] == "commit"]
    pushes = [event for event in events if event[0] == "push"]

    if outcome in {"candidate", "parent"}:
        assert state["pending_rev"] is None
        assert state["receipt_rev"] is not None
        assert state["remote_rev"] == state["receipt_rev"]
        assert private_mutations[0] == ["update-ref", RECEIPT_REF, pending, receipt]
        assert private_mutations[1] == ["delete-ref", PENDING_REF, pending]
        if outcome == "candidate":
            assert state["receipt_rev"] == pending
            assert state["commit_count"] == commit_count
            assert not commits
            assert not pushes
            assert len(private_mutations) == 2
        else:
            replacement = state["receipt_rev"]
            assert replacement != pending
            assert len(commits) == len(pushes) == 1
            assert private_mutations[2:] == [
                [
                    "update-ref",
                    PENDING_REF,
                    state["commits"][replacement]["parent"],
                    "",
                ],
                ["update-ref", RECEIPT_REF, replacement, pending],
                ["delete-ref", PENDING_REF, replacement],
            ]
            assert state["commit_count"] == commit_count + 1
    else:
        assert state["receipt_rev"] == receipt
        assert state["pending_rev"] == pending
        assert state["commit_count"] == commit_count
        assert not private_mutations
        assert not commits
        assert not pushes


class TestDeployLifecycleCheckerKnownBad(unittest.TestCase):
    def test_pending_recovery_checker_rejects_early_private_ref_mutations(self) -> None:
        cases = (
            (
                "receipt promotion",
                [["update-ref", RECEIPT_REF, "pending", "receipt"]],
                {"receipt_rev": "pending", "pending_rev": "pending"},
                "bad_signature",
            ),
            (
                "pending deletion",
                [["delete-ref", PENDING_REF, "pending"]],
                {"receipt_rev": "receipt", "pending_rev": None},
                "wrong_target",
            ),
            (
                "wrong receipt CAS",
                [
                    ["update-ref", RECEIPT_REF, "pending", "other"],
                    ["delete-ref", PENDING_REF, "pending"],
                ],
                {
                    "receipt_rev": "pending",
                    "pending_rev": None,
                    "remote_rev": "pending",
                },
                "candidate",
            ),
        )
        for name, events, refs, outcome in cases:
            with self.subTest(name=name):
                bad = {
                    "events": events,
                    "commit_count": 1,
                    "commits": {},
                    **refs,
                }
                with self.assertRaises(AssertionError):
                    assert_pending_recovery_invariants(
                        bad,
                        receipt="receipt",
                        pending="pending",
                        event_start=0,
                        commit_count=1,
                        outcome=outcome,
                    )

    def test_divergent_checker_rejects_pending_deletion_in_fail_closed_world(self) -> None:
        bad = {
            "events": [
                ["delete-ref", "refs/cratedigger-deploy/cratedigger-src-pending", "pending"],
            ],
            "commits": {},
            "receipt_rev": "receipt",
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

    def test_same_target_checker_rejects_unnecessary_current_pin(self) -> None:
        bad = {
            "events": [["commit", "new"]],
            "commits": {"new": {"parent": "remote", "target": "target"}},
            "receipt_rev": "receipt",
        }
        with self.assertRaises(AssertionError):
            assert_same_target_divergent_invariants(
                bad,
                receipt="receipt",
                remote="remote",
                target="target",
                remote_relation="requested",
                verifier_available=True,
            )

    def test_divergent_checker_rejects_pending_mutation_in_fail_closed_world(self) -> None:
        bad = {
            "events": [
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending", "remote", ""],
            ],
            "commits": {},
            "receipt_rev": "receipt",
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

    def test_divergent_checker_rejects_non_cas_receipt_overwrite(self) -> None:
        bad = {
            "events": [
                ["commit", "new"],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src-pending", "remote", ""],
                ["update-ref", "refs/cratedigger-deploy/cratedigger-src", "new", "other"],
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
                remote_matches_receipt=True,
                verifier_available=True,
            )

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
                "signal_after_pending_commit",
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
            pending = after_first["receipt_rev"] or after_first["pending_rev"]

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
            replacement_recovery = (
                pending is not None
                and remote_after_failure == "other"
                and recovery_verifier == "available"
            )
            assert_deploy_lifecycle_invariants(
                fake.state,
                target=fake.TARGET_REV,
                max_signed_commits=2 if replacement_recovery else 1,
                max_receipt_updates=2 if replacement_recovery else 1,
            )

    def test_divergent_sibling_receipt_is_superseded_only_by_equivalent_verified_remote(
        self,
    ) -> None:
        for remote_matches_receipt in (False, True):
            for verifier_available in (False, True):
                with self.subTest(
                    remote_matches_receipt=remote_matches_receipt,
                    verifier_available=verifier_available,
                ), tempfile.TemporaryDirectory() as tempdir:
                    fake = FakeDeployPinCommands(Path(tempdir))
                    receipt = fake.seed_divergent_receipt(
                        remote_target=(
                            fake.OLD_TARGET
                            if remote_matches_receipt
                            else fake.TARGET_REV
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

    def test_same_target_divergent_receipt_recovery_is_bounded(self) -> None:
        # Exhaustive six-world relation/signature table. The requested,
        # verified-parent, unknown-parent, and verified-other decisive worlds
        # are all retained.
        for remote_relation in ("requested", "parent", "other"):
            for verifier_available in (False, True):
                with self.subTest(
                    remote_relation=remote_relation,
                    verifier_available=verifier_available,
                ), tempfile.TemporaryDirectory() as tempdir:
                    fake = FakeDeployPinCommands(Path(tempdir))
                    remote_target = {
                        "requested": fake.TARGET_REV,
                        "parent": fake.OLD_TARGET,
                        "other": "7" * 40,
                    }[remote_relation]
                    receipt = fake.seed_divergent_receipt(
                        receipt_target=fake.TARGET_REV,
                        remote_target=remote_target,
                    )
                    fake.update_state(
                        remote_signature_status=(
                            "G" if verifier_available else "U"
                        )
                    )

                    fake.run(SCRIPT)

                    assert_same_target_divergent_invariants(
                        fake.state,
                        receipt=receipt,
                        remote=fake.OTHER_REV,
                        target=fake.TARGET_REV,
                        remote_relation=remote_relation,
                        verifier_available=verifier_available,
                    )

    @settings(max_examples=7, deadline=None)
    @given(
        outcome=st.sampled_from(
            (
                "candidate",
                "parent",
                "bad_signature",
                "unknown_signature",
                "unreadable_lock",
                "wrong_target",
                "movement",
            )
        )
    )
    @example(outcome="candidate")
    @example(outcome="parent")
    @example(outcome="bad_signature")
    @example(outcome="unknown_signature")
    @example(outcome="unreadable_lock")
    @example(outcome="wrong_target")
    @example(outcome="movement")
    def test_interrupted_pending_recovery_validates_remote_before_private_mutation(
        self, outcome: str,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            fake = FakeDeployPinCommands(Path(tempdir))
            receipt = fake.seed_divergent_receipt()
            fake.update_state(
                fault="signal_after_pending_commit", remote_move_on_nix=True,
            )
            fake.run(SCRIPT)
            pending = fake.state["pending_rev"]
            self.assertIsNotNone(pending)
            assert pending is not None

            changes: dict = {
                "fault": None,
                "remote_move_on_nix": False,
                "remote_signature_status": "G",
                "remote_lock_readable": True,
                "remote_target": fake.OLD_TARGET,
                "remote_change_on_ls_remote_call": None,
            }
            if outcome == "candidate":
                changes.update(remote_rev=pending, remote_target=fake.TARGET_REV)
            elif outcome == "bad_signature":
                changes["remote_signature_status"] = "B"
            elif outcome == "unknown_signature":
                changes["remote_signature_status"] = "U"
            elif outcome == "unreadable_lock":
                changes["remote_lock_readable"] = False
            elif outcome == "wrong_target":
                changes["remote_target"] = "7" * 40
            elif outcome == "movement":
                changes.update(
                    ls_remote_count=0,
                    remote_change_on_ls_remote_call=2,
                    moved_remote_rev="8" * 40,
                    moved_remote_parent=fake.BASE_REV,
                    moved_remote_target=fake.OLD_TARGET,
                )
            event_start = len(fake.state["events"])
            commit_count = fake.state["commit_count"]
            fake.update_state(**changes)

            retry = fake.run(SCRIPT)
            if outcome in {"candidate", "parent"}:
                self.assertEqual(retry.returncode, 0, retry.stderr)
            else:
                self.assertNotEqual(retry.returncode, 0, retry.stderr)

            assert_pending_recovery_invariants(
                fake.state,
                receipt=receipt,
                pending=pending,
                event_start=event_start,
                commit_count=commit_count,
                outcome=outcome,
            )


if __name__ == "__main__":
    unittest.main()
