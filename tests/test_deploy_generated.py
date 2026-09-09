"""Generated patrol of scripts/deploy.sh's nixos-upgrade wait loop.

The loop decides one thing from a stream of ``systemctl show`` snapshots:
keep waiting, succeed, or fail with a named reason. A pure oracle states that
decision; the property drives the real script through the real-git fixture
in already-pinned mode (no nix, no commit) and requires the two to agree on
every generated stream, including the ones that never resolve and must time
out rather than pass.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from tests.fakes.deploy import (
    NEW_INVOCATION,
    PREVIOUS_INVOCATION,
    FakeDeployWorld,
    upgrade_state,
)

OTHER_INVOCATION = "c" * 32
RUNNING_STATES = ("activating", "active", "reloading", "deactivating")

OUTCOME_MESSAGES = {
    "success": "check your change through the real CLI, API, or UI",
    "disappeared": "disappeared",
    "changed": "invocation changed mid-deploy",
    "failed": "nixos-upgrade failed",
    "unexpected": "unexpected nixos-upgrade state",
    "timeout": "waiting for nixos-upgrade on doc2",
}


def expected_wait_outcome(
    previous: str, snapshots: list[dict[str, str]]
) -> str:
    """The wait loop's decision over ``snapshots``, the last one repeating."""
    triggered = None
    for snapshot in snapshots:
        invocation = snapshot["InvocationID"]
        if invocation in ("", previous):
            if triggered is not None:
                return "disappeared"
            continue
        if triggered is None:
            triggered = invocation
        elif invocation != triggered:
            return "changed"
        active, sub, result = (
            snapshot["ActiveState"], snapshot["SubState"], snapshot["Result"]
        )
        if active == "inactive" and sub == "dead" and result == "success":
            return "success"
        if active in ("failed", "inactive"):
            return "failed"
        if active not in RUNNING_STATES:
            return "unexpected"
    # Exhausted with the last snapshot repeating: still waiting, forever.
    return "timeout"


snapshot_strategy = st.builds(
    upgrade_state,
    st.sampled_from(("", PREVIOUS_INVOCATION, NEW_INVOCATION, OTHER_INVOCATION)),
    st.sampled_from(RUNNING_STATES + ("inactive", "failed", "maintenance")),
    st.sampled_from(("start", "running", "dead", "exited", "failed")),
    st.sampled_from(("success", "exit-code", "timeout")),
)
stream_strategy = st.lists(snapshot_strategy, min_size=1, max_size=6)


class TestOracleKnownBad(unittest.TestCase):
    """Each clause of the oracle trips on the minimal stream that reaches it."""

    def test_success_needs_all_three_terminal_facts(self) -> None:
        stream = [upgrade_state(NEW_INVOCATION, "inactive", "dead", "success")]
        self.assertEqual(expected_wait_outcome(PREVIOUS_INVOCATION, stream), "success")
        for broken in (
            upgrade_state(NEW_INVOCATION, "inactive", "exited", "success"),
            upgrade_state(NEW_INVOCATION, "inactive", "dead", "exit-code"),
        ):
            self.assertEqual(
                expected_wait_outcome(PREVIOUS_INVOCATION, [broken]), "failed"
            )

    def test_previous_invocation_is_waiting_not_failure(self) -> None:
        stream = [upgrade_state(PREVIOUS_INVOCATION, "failed", "failed", "exit-code")]
        self.assertEqual(expected_wait_outcome(PREVIOUS_INVOCATION, stream), "timeout")

    def test_tracked_invocation_vanishing_is_disappeared(self) -> None:
        stream = [
            upgrade_state(NEW_INVOCATION, "active", "running"),
            upgrade_state("", "inactive", "dead"),
        ]
        self.assertEqual(expected_wait_outcome(PREVIOUS_INVOCATION, stream), "disappeared")

    def test_second_new_invocation_is_changed(self) -> None:
        stream = [
            upgrade_state(NEW_INVOCATION, "active", "running"),
            upgrade_state(OTHER_INVOCATION, "inactive", "dead"),
        ]
        self.assertEqual(expected_wait_outcome(PREVIOUS_INVOCATION, stream), "changed")

    def test_unknown_active_state_is_unexpected(self) -> None:
        stream = [upgrade_state(NEW_INVOCATION, "maintenance", "running")]
        self.assertEqual(expected_wait_outcome(PREVIOUS_INVOCATION, stream), "unexpected")


class TestGeneratedWaitLoop(unittest.TestCase):
    @settings(max_examples=40, deadline=None)
    @example([
        upgrade_state(PREVIOUS_INVOCATION, "inactive", "dead"),
        upgrade_state(NEW_INVOCATION, "activating", "start"),
        upgrade_state(NEW_INVOCATION, "inactive", "dead"),
    ])
    @example([upgrade_state(PREVIOUS_INVOCATION, "inactive", "dead")])
    @example([
        upgrade_state(NEW_INVOCATION, "active", "running"),
        upgrade_state("", "inactive", "dead"),
    ])
    @example([
        upgrade_state(NEW_INVOCATION, "active", "running"),
        upgrade_state(OTHER_INVOCATION, "inactive", "dead"),
    ])
    @example([upgrade_state(NEW_INVOCATION, "failed", "failed", "exit-code")])
    @example([upgrade_state(NEW_INVOCATION, "maintenance", "running")])
    @given(stream_strategy)
    def test_real_script_agrees_with_the_oracle(
        self, stream: list[dict[str, str]]
    ) -> None:
        expected = expected_wait_outcome(PREVIOUS_INVOCATION, stream)
        with tempfile.TemporaryDirectory() as tmp:
            world = FakeDeployWorld(Path(tmp))
            world.pin_forgejo_master(world.main_tip)
            world.update_state(upgrade_states=stream)

            # A stream that resolves gets a deadline no loaded host can beat;
            # one the oracle says never resolves must actually time out, so
            # it gets the shortest deadline bash's whole-second clock allows.
            deadline = "1" if expected == "timeout" else "60"
            proc = world.run(
                extra_env={"CRATEDIGGER_DEPLOY_TIMEOUT_SECONDS": deadline},
                timeout=120,
            )

            transcript = proc.stdout + proc.stderr
            self.assertIn(OUTCOME_MESSAGES[expected], transcript)
            self.assertEqual(proc.returncode, 0 if expected == "success" else 1,
                             transcript)
            # A non-success never reports the anchor as activated.
            if expected != "success":
                self.assertNotIn("activated nixosconfig", proc.stdout)


if __name__ == "__main__":
    unittest.main()
