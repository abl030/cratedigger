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

from hypothesis import given
from hypothesis import strategies as st

import cratedigger
import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from lib.config import CratediggerConfig
from lib.context import CratediggerContext
from lib.convergence import (
    CONVERGENCE_STEPS,
    ConvergenceGroup,
    ConvergenceStep,
    resolve_convergence_target,
    run_convergence_steps,
)
from tests.fakes import FakePipelineDB
from tests.helpers import make_ctx_with_fake_db


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
        # The four pre-phase steps (cooldown load, search-plan
        # reconciliation, media-server pin reconcilers) precede the slskd
        # convergence sweeps in the order main() used to hand-roll them.
        # load_user_cooldowns MUST stay ahead of Phase 1's submit:
        # build_phase1_context forwards ctx.cooled_down_users by reference
        # after the loader replaces the set.
        self.assertEqual(
            tuple(step.name for step in CONVERGENCE_STEPS[ConvergenceGroup.PHASE_ZERO]),
            (
                "load_user_cooldowns",
                "reconcile_search_plans_cycle",
                "reconcile_plex_added_at_pins_cycle",
                "reconcile_jellyfin_date_created_pins_cycle",
                "converge_slskd_orphans",
                "reap_disk_orphans",
                "converge_slskd_searches",
                "prune_transfer_ledger_cycle",
                "prune_terminal_pin_rows_cycle",
            ),
        )

    def test_end_of_cycle_harvest_precedes_purge(self):
        # harvest-before-purge is the load-bearing ordering constraint; the
        # three close-out steps (summary line, metrics row, peer roster)
        # follow so a failed render can never block metrics persistence.
        self.assertEqual(
            tuple(
                step.name
                for step in CONVERGENCE_STEPS[ConvergenceGroup.END_OF_CYCLE]
            ),
            (
                "harvest_terminal_transfer_evidence",
                "purge_completed_transfers",
                "log_cycle_summary",
                "record_cycle_metrics_cycle",
                "record_peer_observations_cycle",
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

        import_mock.assert_called_once_with("lib.user_cooldowns")
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

        phase_zero_line = group_lines["PHASE_ZERO"][0]
        end_of_cycle_line = group_lines["END_OF_CYCLE"][0]
        phase1_with_start, phase1_with_end = phase1_with_lines[0]
        self.assertLess(phase_zero_line, phase1_start_lines[0])
        self.assertLess(phase_zero_line, phase1_with_start)
        self.assertGreater(end_of_cycle_line, phase1_with_end)


class TestMainContextWiring(unittest.TestCase):
    """#1178 PR2 review F1 (mutant b): a bounded AST parse of
    ``cratedigger.main`` -- the same technique as
    ``TestMainConvergenceWindows`` above, since ``main()`` needs a live DB
    / slskd client to actually run -- pinning that the per-cycle
    ``_module_ctx`` construction wires a real
    ``lib.enqueue.ClaimedQueueKeysRegistry()`` into
    ``claimed_queue_keys_registry``. Deleting the kwarg (or the whole
    construction) would silently degrade the cross-request enqueue guard
    to its cross-cycle-only layer, which does not reliably catch the
    actual #1178 same-cycle collision: neither sibling request has an
    accepted ledger row yet at the moment the other's guard runs."""

    def test_module_ctx_wires_a_claimed_queue_keys_registry(self):
        tree = ast.parse(inspect.getsource(cratedigger.main))
        matches: list[ast.keyword] = []
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "_module_ctx"
                and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Name)
                and node.value.func.id == "CratediggerContext"
            ):
                continue
            for kw in node.value.keywords:
                if kw.arg == "claimed_queue_keys_registry":
                    matches.append(kw)

        self.assertEqual(
            len(matches), 1,
            "expected exactly one _module_ctx = CratediggerContext(...) "
            "assignment carrying a claimed_queue_keys_registry= kwarg",
        )
        value = matches[0].value
        self.assertTrue(
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "ClaimedQueueKeysRegistry",
            f"claimed_queue_keys_registry= must construct a real "
            f"ClaimedQueueKeysRegistry(), got {ast.dump(value)}",
        )


class TestPhase1ContextCallSite(unittest.TestCase):
    """#1278 review: the SAME bounded-AST technique, pinning the site the
    defect actually shipped at.

    `build_phase1_context` forwards `download_ownership` and
    `tests/test_cycle_summary.py::TestPhase1ContextForwarding` pins that
    it does. Neither constrains `main()` to CALL it — and the shipped
    defect was exactly that: an inline `CratediggerContext(...)` in
    `_run_phase1` missing the kwarg, which made every download-timeout
    cleanup a no-op under the ownership gate. Reverting that one line
    reproduces the production bug with the whole suite green.
    """

    def test_phase1_ctx_is_built_by_the_forwarding_helper(self):
        tree = ast.parse(inspect.getsource(cratedigger.main))
        assignments = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "phase1_ctx"
        ]

        self.assertEqual(
            len(assignments), 1,
            "expected exactly one phase1_ctx = ... assignment in main()",
        )
        value = assignments[0].value
        self.assertTrue(
            isinstance(value, ast.Call)
            and isinstance(value.func, ast.Name)
            and value.func.id == "build_phase1_context",
            "phase1_ctx must be built by build_phase1_context(...), never "
            "by an inline CratediggerContext(...) — an inline construction "
            "silently drops whatever collaborator the next author forgets, "
            f"got {ast.dump(value)}",
        )


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
            for name, should_raise in zip(names, raises, strict=True)
        )
        log = MagicMock()

        run_convergence_steps(
            cast(CratediggerContext, object()), steps, log=log)

        assert_all_steps_attempted_in_order(names, tuple(attempted))
        self.assertEqual(log.exception.call_count, sum(raises))


def _registered_step(name: str) -> ConvergenceStep:
    for steps in CONVERGENCE_STEPS.values():
        for step in steps:
            if step.name == name:
                return step
    raise AssertionError(f"no registered convergence step named {name!r}")


def _raising_db(failing_method: str) -> FakePipelineDB:
    """FakePipelineDB whose named method raises — the collaborator-down world."""
    db = FakePipelineDB()

    def _boom(*_args: object, **_kwargs: object) -> object:
        raise RuntimeError(f"{failing_method} down")

    setattr(db, failing_method, _boom)
    return db


class TestRegisteredStepFailureMessagesReachable(unittest.TestCase):
    """Per-step proof that each registered failure message is reachable.

    A step that swallows its own collaborator failure turns the registered
    ``failure_message`` into dead copy and demotes the failure to a warning
    (the pre-fix ``prune_transfer_ledger_cycle`` shape, plus the two pin
    reconcilers' fetch-level catches). Each row builds the minimal world in
    which the step's first collaborator touch raises, runs the REAL
    registered step through the runner, and asserts the runner logged THAT
    step's message.

    Scope: the steps this series registered or corrected. The slskd
    convergence sweeps (orphans, disk reap, search ledger, harvest, purge)
    keep their own internal isolation contracts and belong to the
    owned-key-module work (#1278 strong candidate 1), not this table.
    """

    def _ctx(self, failing_method: str):
        return make_ctx_with_fake_db(
            _raising_db(failing_method), cfg=CratediggerConfig())

    def _cases(self):
        plex_ctx = make_ctx_with_fake_db(
            _raising_db("get_pending_plex_added_at_pins"),
            cfg=CratediggerConfig(plex_url="http://plex:32400"))
        jellyfin_ctx = make_ctx_with_fake_db(
            _raising_db("get_pending_jellyfin_date_created_pins"),
            cfg=CratediggerConfig(
                jellyfin_url="http://jellyfin:8096", jellyfin_token="tok"))
        summary_ctx = make_ctx_with_fake_db(
            FakePipelineDB(), cfg=CratediggerConfig())
        summary_ctx.cycle_start = None  # time.time() - None raises TypeError
        observations_ctx = self._ctx("record_peer_observations")
        observations_ctx.peer_observations = {"peer-a"}
        return [
            ("load_user_cooldowns", self._ctx("get_cooled_down_users")),
            ("reconcile_search_plans_cycle",
             self._ctx("list_wanted_for_plan_reconciliation")),
            ("reconcile_plex_added_at_pins_cycle", plex_ctx),
            ("reconcile_jellyfin_date_created_pins_cycle", jellyfin_ctx),
            ("prune_transfer_ledger_cycle", self._ctx("prune_transfer_ledger")),
            ("prune_terminal_pin_rows_cycle",
             self._ctx("prune_terminal_plex_added_at_pins")),
            ("log_cycle_summary", summary_ctx),
            ("record_cycle_metrics_cycle", self._ctx("record_cycle_metrics")),
            ("record_peer_observations_cycle", observations_ctx),
        ]

    def test_each_step_failure_logs_its_registered_message(self):
        for name, ctx in self._cases():
            with self.subTest(step=name):
                step = _registered_step(name)
                log = MagicMock()
                run_convergence_steps(ctx, (step,), log=log)
                log.exception.assert_called_once_with(step.failure_message)


class TestConvergenceCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test proving the orchestration checker constrains."""

    def test_checker_trips_when_a_raising_step_blocks_the_next_step(self):
        with self.assertRaises(AssertionError):
            assert_all_steps_attempted_in_order(
                ("first", "raising", "last"), ("first", "raising"))


if __name__ == "__main__":
    unittest.main()
