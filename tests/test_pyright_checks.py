"""Contracts for the concurrent complementary Pyright runner."""

from __future__ import annotations

import io
import threading
import unittest
from pathlib import Path

from scripts.run_pyright_checks import (
    PyrightCheck,
    PyrightOutcome,
    combined_exit_code,
    execute_checks,
    recommended_thread_counts,
    write_outcomes,
)


class TestPyrightThreadAllocation(unittest.TestCase):
    def test_measured_doc1_allocation_caps_at_eight_plus_four(self) -> None:
        self.assertEqual(recommended_thread_counts(30), (8, 4))
        self.assertEqual(recommended_thread_counts(12), (8, 4))

    def test_smaller_hosts_share_the_available_cpu_budget(self) -> None:
        self.assertEqual(recommended_thread_counts(8), (5, 3))
        self.assertEqual(recommended_thread_counts(4), (3, 1))
        self.assertEqual(recommended_thread_counts(2), (1, 1))
        self.assertEqual(recommended_thread_counts(1), (1, 1))


class TestConcurrentPyrightExecution(unittest.TestCase):
    def test_both_contracts_start_before_either_is_allowed_to_finish(self) -> None:
        barrier = threading.Barrier(2, timeout=2)
        started: list[str] = []
        lock = threading.Lock()
        checks = (
            PyrightCheck("whole-tree", "pyrightconfig.json", 8),
            PyrightCheck("production-strict", "pyrightconfig.production.json", 4),
        )

        def run(check: PyrightCheck) -> PyrightOutcome:
            with lock:
                started.append(check.name)
            barrier.wait()
            return PyrightOutcome(check=check, returncode=0, output="clean\n")

        outcomes = execute_checks(checks, run=run)

        self.assertCountEqual(started, ["whole-tree", "production-strict"])
        self.assertEqual(tuple(outcome.check for outcome in outcomes), checks)

    def test_failure_does_not_cancel_the_other_contract(self) -> None:
        executed: list[str] = []
        checks = (
            PyrightCheck("whole-tree", "pyrightconfig.json", 8),
            PyrightCheck("production-strict", "pyrightconfig.production.json", 4),
        )

        def run(check: PyrightCheck) -> PyrightOutcome:
            executed.append(check.name)
            return PyrightOutcome(
                check=check,
                returncode=1 if check.name == "whole-tree" else 0,
                output=f"{check.name} output\n",
            )

        outcomes = execute_checks(checks, run=run)

        self.assertCountEqual(executed, ["whole-tree", "production-strict"])
        self.assertEqual(combined_exit_code(outcomes), 1)
        stream = io.StringIO()
        write_outcomes(outcomes, stream)
        rendered = stream.getvalue()
        self.assertIn("=== whole-tree Pyright (8 threads) ===", rendered)
        self.assertIn("whole-tree output", rendered)
        self.assertIn("=== production-strict Pyright (4 threads) ===", rendered)
        self.assertIn("production-strict output", rendered)

    def test_non_diagnostic_process_failure_is_infrastructure_failure(self) -> None:
        outcomes = (
            PyrightOutcome(
                PyrightCheck("whole-tree", "pyrightconfig.json", 8),
                returncode=0,
                output="clean\n",
            ),
            PyrightOutcome(
                PyrightCheck(
                    "production-strict",
                    "pyrightconfig.production.json",
                    4,
                ),
                returncode=127,
                output="pyright missing\n",
            ),
        )

        self.assertEqual(combined_exit_code(outcomes), 2)

    def test_unrecognized_configuration_is_never_a_green_check(self) -> None:
        outcomes = (
            PyrightOutcome(
                PyrightCheck("whole-tree", "pyrightconfig.json", 8),
                returncode=0,
                output='Config contains unrecognized setting "invented".\n',
            ),
        )

        self.assertEqual(combined_exit_code(outcomes), 2)

    def test_production_config_contains_only_recognized_rules(self) -> None:
        source = Path("pyrightconfig.production.json").read_text(encoding="utf-8")

        self.assertNotIn("reportShadowedImports", source)


if __name__ == "__main__":
    unittest.main()
