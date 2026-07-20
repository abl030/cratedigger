"""Generated lifecycle patrol for exact timer-driven cycle verification."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from hypothesis import example, given, settings
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401 - registers active profile
from tests.fakes.deploy_cycle import FakeDeployCycleCommands


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "verify_cratedigger_cycle.sh"


def assert_exact_cycle_invariants(
    records: list[dict[str, Any]],
    *,
    invocation: str,
    expected_source: str,
) -> None:
    """Require exact source, application completion, and manager success."""

    expected_script = f"{expected_source}/cratedigger.py"
    source = any(
        record.get("_SYSTEMD_INVOCATION_ID") == invocation
        and expected_script in str(record.get("_CMDLINE", "")).split()
        for record in records
    )
    completed = any(
        record.get("_SYSTEMD_INVOCATION_ID") == invocation
        and "Cratedigger cycle complete" in str(record.get("MESSAGE", ""))
        for record in records
    )
    deactivated = any(
        record.get("INVOCATION_ID") == invocation
        and record.get("MESSAGE")
        == "cratedigger.service: Deactivated successfully."
        for record in records
    )
    finished = any(
        record.get("INVOCATION_ID") == invocation
        and record.get("MESSAGE")
        == "Finished Cratedigger — Soulseek download pipeline."
        and record.get("JOB_TYPE") == "start"
        and record.get("JOB_RESULT") == "done"
        for record in records
    )
    failed = any(
        record.get("INVOCATION_ID") == invocation
        and (
            record.get("JOB_RESULT") not in (None, "done")
            or "Failed" in str(record.get("MESSAGE", ""))
        )
        for record in records
    )

    assert source, "expected source record absent"
    assert completed, "application completion absent"
    assert deactivated, "successful deactivation absent"
    assert finished, "successful finished job absent"
    assert not failed, "failure evidence present"


class TestExactCycleCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_next_invocation_as_target_proof(self) -> None:
        fake = FakeDeployCycleCommands
        records = fake.success_records(invocation=fake.NEXT)
        with self.assertRaises(AssertionError):
            assert_exact_cycle_invariants(
                records,
                invocation=fake.TARGET,
                expected_source=fake.SOURCE,
            )

    def test_checker_rejects_application_only_success(self) -> None:
        fake = FakeDeployCycleCommands
        records = [
            fake.source_record(),
            {
                "_SYSTEMD_INVOCATION_ID": fake.TARGET,
                "MESSAGE": "Cratedigger cycle complete in 1.0s",
            },
        ]
        with self.assertRaises(AssertionError):
            assert_exact_cycle_invariants(
                records,
                invocation=fake.TARGET,
                expected_source=fake.SOURCE,
            )

    def test_checker_rejects_failure_evidence_beside_success_markers(self) -> None:
        fake = FakeDeployCycleCommands
        records = [
            *fake.success_records(),
            {
                "INVOCATION_ID": fake.TARGET,
                "JOB_RESULT": "failed",
                "JOB_TYPE": "start",
                "MESSAGE": "Failed to start Cratedigger.",
            },
        ]
        with self.assertRaises(AssertionError):
            assert_exact_cycle_invariants(
                records,
                invocation=fake.TARGET,
                expected_source=fake.SOURCE,
            )


class TestGeneratedExactCycleVerifier(unittest.TestCase):
    @settings(max_examples=20, deadline=None)
    @given(
        old_source_first=st.booleans(),
        failed_target=st.booleans(),
        later_healthy_target_source=st.booleans(),
    )
    @example(
        old_source_first=False,
        failed_target=True,
        later_healthy_target_source=True,
    )
    def test_capture_chooses_first_target_source_start_from_journal_history(
        self,
        old_source_first: bool,
        failed_target: bool,
        later_healthy_target_source: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            fake = FakeDeployCycleCommands(Path(tempdir))
            starts: list[dict[str, str]] = []
            journals: dict[str, list[list[dict[str, str]]]] = {}
            if old_source_first:
                starts.append(fake.start_record(fake.OLD_SUCCESSOR))
                journals[fake.OLD_SUCCESSOR] = [[fake.source_record(
                    invocation=fake.OLD_SUCCESSOR,
                    source="/nix/store/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-source",
                )]]
            target_records = [fake.source_record()]
            if failed_target:
                target_records.append({
                    "INVOCATION_ID": fake.TARGET,
                    "JOB_RESULT": "failed",
                    "JOB_TYPE": "start",
                    "MESSAGE": "Failed to start Cratedigger.",
                })
            starts.append(fake.start_record(fake.TARGET))
            journals[fake.TARGET] = [target_records]
            starts.append(fake.start_record(fake.NEXT))
            later_source = (
                fake.SOURCE
                if later_healthy_target_source
                else "/nix/store/cccccccccccccccccccccccccccccccc-source"
            )
            journals[fake.NEXT] = [[fake.source_record(
                invocation=fake.NEXT,
                source=later_source,
            )]]
            fake.write_state(
                system_states=[
                    fake.system_state(fake.OLD),
                    fake.system_state(fake.NEXT),
                ],
                journal_snapshots=journals,
                start_journal_snapshots=[starts],
            )

            proc = fake.run(
                SCRIPT,
                "capture-target",
                fake.CURSOR,
                fake.SOURCE,
            )

            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertEqual(proc.stdout.strip(), fake.TARGET)

    @settings(max_examples=40, deadline=None)
    @given(
        source=st.booleans(),
        completed=st.booleans(),
        deactivated=st.booleans(),
        finished=st.booleans(),
        explicit_failure=st.booleans(),
        rolled_over=st.booleans(),
    )
    @example(
        source=True,
        completed=True,
        deactivated=True,
        finished=True,
        explicit_failure=False,
        rolled_over=True,
    )
    @example(
        source=True,
        completed=True,
        deactivated=False,
        finished=False,
        explicit_failure=False,
        rolled_over=True,
    )
    @example(
        source=True,
        completed=True,
        deactivated=False,
        finished=True,
        explicit_failure=False,
        rolled_over=True,
    )
    @example(
        source=True,
        completed=True,
        deactivated=True,
        finished=False,
        explicit_failure=False,
        rolled_over=True,
    )
    def test_real_script_accepts_exactly_complete_successful_target_worlds(
        self,
        source: bool,
        completed: bool,
        deactivated: bool,
        finished: bool,
        explicit_failure: bool,
        rolled_over: bool,
    ) -> None:
        with tempfile.TemporaryDirectory() as tempdir:
            fake = FakeDeployCycleCommands(Path(tempdir))
            records: list[dict[str, str]] = []
            if source:
                records.append(fake.source_record())
            if completed:
                records.append({
                    "_SYSTEMD_INVOCATION_ID": fake.TARGET,
                    "MESSAGE": "Cratedigger cycle complete in 1.0s",
                })
            if deactivated:
                records.append({
                    "INVOCATION_ID": fake.TARGET,
                    "MESSAGE": "cratedigger.service: Deactivated successfully.",
                })
            if finished:
                records.append({
                    "INVOCATION_ID": fake.TARGET,
                    "JOB_RESULT": "done",
                    "JOB_TYPE": "start",
                    "MESSAGE": "Finished Cratedigger — Soulseek download pipeline.",
                })
            if explicit_failure:
                records.append({
                    "INVOCATION_ID": fake.TARGET,
                    "JOB_RESULT": "failed",
                    "JOB_TYPE": "start",
                    "MESSAGE": "Failed to start Cratedigger.",
                })

            checker_accepts = True
            try:
                assert_exact_cycle_invariants(
                    records,
                    invocation=fake.TARGET,
                    expected_source=fake.SOURCE,
                )
            except AssertionError:
                checker_accepts = False

            current = fake.NEXT if rolled_over else fake.TARGET
            fake.write_state(
                system_states=[fake.system_state(current)],
                journal_snapshots={fake.TARGET: [records]},
            )
            proc = fake.run(
                SCRIPT,
                "verify-exact",
                fake.TARGET,
                fake.SOURCE,
                max_polls=2,
            )

            self.assertEqual(
                proc.returncode == 0,
                checker_accepts,
                proc.stdout + proc.stderr,
            )


if __name__ == "__main__":
    unittest.main()
