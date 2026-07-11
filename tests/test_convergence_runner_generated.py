#!/usr/bin/env python3
"""Pinned + generated orchestration invariants for convergence steps.

The registry is production ordering data.  The runner must attempt every step
in that order even when any subset raises; cleanup is best-effort and can
never abort the album-processing cycle.  The end-of-cycle registry also owns
the pre-purge evidence-harvest ordering constraint explicitly.
"""
from __future__ import annotations

import ast
import inspect
import os
import sys
import unittest
from collections.abc import Callable
from typing import cast
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)

import cratedigger
from hypothesis import given
from hypothesis import strategies as st

from lib.context import CratediggerContext
from lib.convergence import (
    CONVERGENCE_STEPS,
    ConvergenceGroup,
    ConvergenceStep,
    resolve_convergence_target,
    run_convergence_steps,
)


def assert_all_steps_attempted_in_order(
    expected: tuple[str, ...], attempted: tuple[str, ...],
) -> None:
    """Every registered step is attempted exactly once in declared order."""
    if attempted != expected:
        raise AssertionError(
            f"expected convergence attempts {expected!r}, got {attempted!r}")


def _recording_step(
    name: str,
    *,
    raises: bool,
    attempted: list[str],
) -> Callable[[CratediggerContext], None]:
    def run(_ctx: CratediggerContext) -> None:
        attempted.append(name)
        if raises:
            raise RuntimeError(f"{name} failed")

    return run


class TestConvergenceRegistryPins(unittest.TestCase):
    """Ordering is pinned from registry data, never source inspection."""

    def test_phase_zero_order_is_explicit(self):
        self.assertEqual(
            tuple(step.name for step in CONVERGENCE_STEPS[ConvergenceGroup.PHASE_ZERO]),
            (
                "converge_slskd_orphans",
                "reap_disk_orphans",
                "converge_slskd_searches",
                "prune_transfer_ledger_cycle",
            ),
        )

    def test_end_of_cycle_harvest_precedes_purge(self):
        self.assertEqual(
            tuple(
                step.name
                for step in CONVERGENCE_STEPS[ConvergenceGroup.END_OF_CYCLE]
            ),
            (
                "harvest_terminal_transfer_evidence",
                "purge_completed_transfers",
            ),
        )

    def test_every_production_target_resolves_to_a_callable(self):
        for group, steps in CONVERGENCE_STEPS.items():
            for step in steps:
                with self.subTest(group=group.value, step=step.name):
                    self.assertIsNotNone(step.module_name)
                    self.assertIsNotNone(step.callable_name)
                    assert step.module_name is not None
                    assert step.callable_name is not None
                    self.assertTrue(callable(resolve_convergence_target(
                        step.module_name, step.callable_name)))

    def test_raising_step_does_not_block_later_steps(self):
        attempted: list[str] = []
        steps = (
            ConvergenceStep(
                name="first",
                run=_recording_step("first", raises=False, attempted=attempted),
                failure_message="first failed",
            ),
            ConvergenceStep(
                name="raising",
                run=_recording_step("raising", raises=True, attempted=attempted),
                failure_message="raising failed",
            ),
            ConvergenceStep(
                name="last",
                run=_recording_step("last", raises=False, attempted=attempted),
                failure_message="last failed",
            ),
        )

        run_convergence_steps(
            cast(CratediggerContext, object()), steps, log=MagicMock())

        assert_all_steps_attempted_in_order(
            ("first", "raising", "last"), tuple(attempted))

    def test_lazy_import_failure_does_not_block_later_steps(self):
        attempted: list[str] = []
        steps = (
            CONVERGENCE_STEPS[ConvergenceGroup.PHASE_ZERO][0],
            ConvergenceStep(
                name="after",
                run=_recording_step(
                    "after", raises=False, attempted=attempted),
                failure_message="after failed",
            ),
        )

        with patch(
            "lib.convergence.import_module",
            side_effect=ImportError("dependency unavailable"),
        ) as import_mock:
            run_convergence_steps(
                cast(CratediggerContext, object()), steps, log=MagicMock())

        import_mock.assert_called_once_with("lib.slskd_transfers")
        assert_all_steps_attempted_in_order(
            ("after",), tuple(attempted))


class TestMainConvergenceWindows(unittest.TestCase):
    """Minimal production integration pin for the two group call sites."""

    def test_main_calls_both_groups_in_their_required_windows(self):
        tree = ast.parse(inspect.getsource(cratedigger.main))
        group_lines: dict[str, list[int]] = {
            "PHASE_ZERO": [],
            "END_OF_CYCLE": [],
        }
        phase1_start_lines: list[int] = []
        phase1_with_lines: list[tuple[int, int]] = []
        summary_lines: list[int] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "run_convergence_group"
                    and len(node.args) >= 2
                    and isinstance(node.args[1], ast.Attribute)
                    and node.args[1].attr in group_lines
                ):
                    group_lines[node.args[1].attr].append(node.lineno)
                if (
                    isinstance(node.func, ast.Attribute)
                    and isinstance(node.func.value, ast.Name)
                    and node.func.value.id == "logger"
                    and node.func.attr == "info"
                    and node.args
                    and isinstance(node.args[0], ast.Constant)
                    and node.args[0].value
                    == "Starting Phase 1 (poll downloads) in background..."
                ):
                    phase1_start_lines.append(node.lineno)
                if (
                    isinstance(node.func, ast.Name)
                    and node.func.id == "format_cycle_summary"
                ):
                    summary_lines.append(node.lineno)
            if isinstance(node, ast.With) and any(
                isinstance(child, ast.Name) and child.id == "phase1_future"
                for child in ast.walk(node)
            ):
                assert node.end_lineno is not None
                phase1_with_lines.append((node.lineno, node.end_lineno))

        self.assertEqual(len(group_lines["PHASE_ZERO"]), 1)
        self.assertEqual(len(group_lines["END_OF_CYCLE"]), 1)
        self.assertEqual(len(phase1_start_lines), 1)
        self.assertEqual(len(phase1_with_lines), 1)
        self.assertEqual(len(summary_lines), 1)

        phase_zero_line = group_lines["PHASE_ZERO"][0]
        end_of_cycle_line = group_lines["END_OF_CYCLE"][0]
        phase1_with_start, phase1_with_end = phase1_with_lines[0]
        self.assertLess(phase_zero_line, phase1_start_lines[0])
        self.assertLess(phase_zero_line, phase1_with_start)
        self.assertGreater(end_of_cycle_line, phase1_with_end)
        self.assertLess(end_of_cycle_line, summary_lines[0])


class TestGeneratedConvergenceIsolation(unittest.TestCase):
    @given(raises=st.lists(st.booleans(), min_size=0, max_size=12))
    def test_arbitrary_raising_steps_never_abort_the_registry(self, raises):
        attempted: list[str] = []
        names = tuple(f"step-{index}" for index in range(len(raises)))
        steps = tuple(
            ConvergenceStep(
                name=name,
                run=_recording_step(
                    name, raises=should_raise, attempted=attempted),
                failure_message=f"{name} failed",
            )
            for name, should_raise in zip(names, raises)
        )
        log = MagicMock()

        run_convergence_steps(
            cast(CratediggerContext, object()), steps, log=log)

        assert_all_steps_attempted_in_order(names, tuple(attempted))
        self.assertEqual(log.exception.call_count, sum(raises))


class TestConvergenceCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test proving the orchestration checker constrains."""

    def test_checker_trips_when_a_raising_step_blocks_the_next_step(self):
        with self.assertRaises(AssertionError):
            assert_all_steps_attempted_in_order(
                ("first", "raising", "last"), ("first", "raising"))


if __name__ == "__main__":
    unittest.main()
