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
from datetime import UTC, datetime
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
from tests.fakes import FakePipelineDB, FakePipelineDBSource, FakeSlskdAPI
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
        # load_user_cooldowns MUST stay ahead of Phase 1's submit so the
        # roster is loaded before either phase reads it; the loader
        # mutates ctx.cooled_down_users in place, keeping
        # build_phase1_context's by-reference forward coherent.
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


class TestCycleConvergenceWindows(unittest.TestCase):
    """Production pin for the two group call sites in ``run_cycle``.

    ``run_cycle`` is executable with fakes (TestRunCycleExecutable below
    drives it for real); this bounded AST pin keeps the WINDOW claim —
    Phase 0 strictly before the Phase-1/Phase-2 block, end-of-cycle
    strictly after it — plus the claim that ``main()`` hands off to
    ``run_cycle`` exactly once, which no behavioral test can reach because
    ``main()`` still needs a live DB and slskd client.
    """

    def test_run_cycle_calls_both_groups_in_their_required_windows(self):
        tree = ast.parse(inspect.getsource(cratedigger.run_cycle))
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

    def test_main_hands_off_to_run_cycle_exactly_once(self):
        tree = ast.parse(inspect.getsource(cratedigger.main))
        calls = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "run_cycle"
        ]
        self.assertEqual(
            len(calls), 1,
            "main() must run exactly one cycle via run_cycle(ctx)")

    def test_dry_run_gate_precedes_the_cycle_handoff(self):
        # A count alone would let run_cycle(ctx) move ABOVE the dry-run
        # gate, silently breaking --reconcile-dry-run's read-only contract
        # while keeping the count at 1 (review F2). Pin the position.
        tree = ast.parse(inspect.getsource(cratedigger.main))
        gate_lines = [
            node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.If)
            and any(
                isinstance(child, ast.Attribute)
                and child.attr == "reconcile_dry_run"
                for child in ast.walk(node.test)
            )
        ]
        run_cycle_lines = [
            node.lineno for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "run_cycle"
        ]
        self.assertEqual(len(gate_lines), 1)
        self.assertEqual(len(run_cycle_lines), 1)
        self.assertLess(gate_lines[0], run_cycle_lines[0])


class TestMainContextWiring(unittest.TestCase):
    """#1178 PR2 review F1 (mutant b): a bounded AST parse of
    ``cratedigger.main`` -- the same technique as
    ``TestCycleConvergenceWindows`` above, since ``main()`` needs a live DB
    / slskd client to actually run -- pinning that the per-cycle
    owner-``ctx`` construction wires a real
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
                and node.targets[0].id == "ctx"
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
            "expected exactly one ctx = CratediggerContext(...) "
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
        tree = ast.parse(inspect.getsource(cratedigger._run_phase1))
        assignments = [
            node for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
            and node.targets[0].id == "phase1_ctx"
        ]

        self.assertEqual(
            len(assignments), 1,
            "expected exactly one phase1_ctx = ... assignment in _run_phase1",
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

    Scope: the steps this series registered or corrected, plus
    ``prune_terminal_pin_rows_cycle`` — already registered and already
    propagating before this series, included as regression patrol so its
    row cannot silently regain a swallow. The slskd convergence sweeps
    (orphans, disk reap, search ledger, harvest, purge) keep their own
    internal isolation contracts and belong to the owned-key-module work
    (#1278 strong candidate 1), not this table.

    One row is fail-closed LEGISLATION rather than reachability proof:
    ``log_cycle_summary``'s world (``ctx.cycle_start = None``) is not
    producible by ``run_cycle``, which always assigns a float — the row
    proves the runner logs the message when the step raises, guarding any
    future summary implementation, not that production can currently reach
    it (the ``assert_quarantine_verdict_is_earned`` pattern).
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


class TestReconcileDryRun(unittest.TestCase):
    """--reconcile-dry-run's helper is read-only and always exits 0."""

    def test_dry_run_reconciles_read_only_and_exits_zero(self):
        db = FakePipelineDB()
        source = FakePipelineDBSource(db)
        with self.assertLogs(
                "lib.startup_reconciliation", level="INFO") as captured:
            self.assertEqual(cratedigger._reconcile_dry_run(source), 0)
        self.assertTrue(any(
            "dry_run=true" in line for line in captured.output))
        # Read-only: no convergence step ran, nothing was persisted.
        self.assertEqual(db.cycle_metrics, [])
        self.assertEqual(db.peer_observations, {})
        self.assertEqual(db.search_plans, {})

    def test_dry_run_failure_logs_its_own_copy_and_still_exits_zero(self):
        source = FakePipelineDBSource(
            _raising_db("list_wanted_for_plan_reconciliation"))
        with self.assertLogs("cratedigger", level="ERROR") as captured:
            self.assertEqual(cratedigger._reconcile_dry_run(source), 0)
        self.assertTrue(any(
            "no summary produced" in line for line in captured.output))


def _cycle_cfg():
    """Production config shape for a fake-driven cycle.

    Media-server backends are CONFIGURED so both pin reconcilers actually
    reach their pending-pin fetch instead of early-returning unconfigured
    (review F1: with the defaults, the two steps whose failure semantics
    this series changed were no-ops in every cycle-level test). With zero
    pending pins neither reconciler makes an HTTP call.
    """
    import configparser
    from dataclasses import replace
    cfg = CratediggerConfig.from_ini(configparser.ConfigParser())
    return replace(
        cfg,
        plex_url="http://plex:32400",
        jellyfin_url="http://jellyfin:8096",
        jellyfin_token="tok",
    )


def _cycle_ctx(db, *, cfg=None):
    ctx = make_ctx_with_fake_db(db, cfg=cfg or _cycle_cfg(), slskd=FakeSlskdAPI())
    return ctx


class TestRunCycleExecutable(unittest.TestCase):
    """The extracted cycle body executes end to end against fakes.

    This is the test the pre-extraction ``main()`` could never have: the
    real Phase 0 registry, the real Phase 1 thread (own fake source via the
    kwarg-DI factory), the real Phase 2 wanted scan, and the real
    end-of-cycle close-out — all over ``FakePipelineDB``/``FakeSlskdAPI``,
    asserting persisted domain state rather than source order.
    """

    def test_clean_world_full_cycle_persists_close_out_state(self):
        from datetime import timedelta

        db = FakePipelineDB()
        db.add_cooldown(
            "cool-peer",
            cooldown_until=datetime.now(UTC) + timedelta(hours=1))
        ctx = _cycle_ctx(db)
        # Seed one browsed peer so the observation flush is a real DB write
        # in this composition, not a guard-skipped no-op (review F1).
        ctx.peer_observations.add("observed-peer")

        phase1_sources: list[FakePipelineDBSource] = []

        def factory(cfg):
            source = FakePipelineDBSource(FakePipelineDB())
            phase1_sources.append(source)
            return source

        # Two loggers: every step module logs to "cratedigger" except
        # lib/startup_reconciliation.py, which uses __name__ (review F10) —
        # without the second guard its per-row ERRORs would escape.
        with self.assertNoLogs("cratedigger", level="ERROR"), \
                self.assertNoLogs("lib.startup_reconciliation", level="ERROR"):
            cratedigger.run_cycle(ctx, phase1_source_factory=factory)

        # Phase 0 ran: the cooldown loader filled the roster in place.
        self.assertEqual(ctx.cooled_down_users, {"cool-peer"})
        # Phase 1 ran on its own source, and the source was closed.
        self.assertEqual(len(phase1_sources), 1)
        self.assertEqual(phase1_sources[0].close_calls, 1)
        # End-of-cycle close-out persisted the metrics row with the
        # ctx-anchored start time, and flushed the peer roster.
        self.assertEqual(len(db.cycle_metrics), 1)
        self.assertEqual(
            db.cycle_metrics[0]["started_at"], ctx.cycle_started_at)
        self.assertIsNotNone(ctx.cycle_started_at)
        self.assertEqual(len(db.peer_observations), 1)


#: DB methods touched only by best-effort registered steps — never by the
#: Phase 1/Phase 2 spine — so a failure in any subset must be isolated.
#: The pin-fetch pair is reachable because _cycle_cfg configures both
#: media-server backends; record_peer_observations because the property
#: seeds one browsed peer (review F1).
_STEP_DB_METHODS = (
    "get_cooled_down_users",
    "list_wanted_for_plan_reconciliation",
    "get_pending_plex_added_at_pins",
    "get_pending_jellyfin_date_created_pins",
    "prune_transfer_ledger",
    "prune_terminal_plex_added_at_pins",
    "prune_terminal_jellyfin_date_created_pins",
    "record_cycle_metrics",
    "record_peer_observations",
)


class TestGeneratedCycleSurvivesStepFailures(unittest.TestCase):
    """Invariant: no best-effort step failure prevents the search phase.

    The registry-level isolation property above proves the runner attempts
    every step; this cycle-level property proves the COMPOSITION — an
    arbitrary subset of step-touched DB methods raising still yields a
    completed ``run_cycle`` whose Phase 2 wanted scan executed.
    """

    @given(failing=st.sets(st.sampled_from(_STEP_DB_METHODS)))
    def test_arbitrary_step_db_failures_never_prevent_phase2(self, failing):
        db = FakePipelineDB()
        for name in failing:
            def _boom(*_args: object, _name: str = name, **_kwargs: object) -> object:
                raise RuntimeError(f"{_name} down")
            setattr(db, name, _boom)
        ctx = _cycle_ctx(db)
        # One browsed peer so the observation flush reaches the DB in
        # every world instead of guard-skipping (review F1).
        ctx.peer_observations.add("observed-peer")
        wanted_scans: list[str] = []
        source = ctx.pipeline_db_source
        real_get_wanted = source.get_wanted_searchable

        def _recording_get_wanted(generator_id, limit=None, *, title_blacklist=()):
            wanted_scans.append(generator_id)
            return real_get_wanted(
                generator_id, limit, title_blacklist=title_blacklist)

        source.get_wanted_searchable = _recording_get_wanted

        cratedigger.run_cycle(
            ctx,
            phase1_source_factory=lambda cfg: FakePipelineDBSource(
                FakePipelineDB()),
        )

        self.assertEqual(len(wanted_scans), 1)
        if "record_cycle_metrics" not in failing:
            self.assertEqual(len(db.cycle_metrics), 1)


class TestConvergenceCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-test proving the orchestration checker constrains."""

    def test_checker_trips_when_a_raising_step_blocks_the_next_step(self):
        with self.assertRaises(AssertionError):
            assert_all_steps_attempted_in_order(
                ("first", "raising", "last"), ("first", "raising"))


if __name__ == "__main__":
    unittest.main()
