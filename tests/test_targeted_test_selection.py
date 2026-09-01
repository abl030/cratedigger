"""Deterministic contracts for targeted plus adjacent test selection."""

from __future__ import annotations

import fcntl
import os
import select
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_python_tests import (
    _parser as _run_python_tests_parser,
)
from scripts.run_python_tests import (
    complete_test_modules,
    discover_test_modules,
    select_test_targets,
)
from scripts.run_targeted_tests import targeted_phases
from scripts.targeted_test_selection import (
    ALWAYS_AMBIENT_TESTS,
    EXACT_PATH_NEIGHBOURS,
    SHARED_MODULES_WITHOUT_COVERAGE,
    _assert_no_double_registration,
    _changed_path_neighbours,
    ambient_test_modules,
    assert_selection_complete,
    expand_test_selection,
)
from scripts.test_substrate import admission_lock_path
from tests._source_pins import pinned_source

REPO_ROOT = Path(__file__).resolve().parents[1]
#: The runner's own --pattern default (scripts/run_python_tests.py), sourced
#: rather than duplicated so this pin can never silently drift from what
#: discover_test_modules actually uses in production.
_DISCOVERY_PATTERN = _run_python_tests_parser().get_default("pattern")


class TestTargetedTestSelection(unittest.TestCase):
    def test_production_test_adds_its_generated_sibling_and_global_ratchets(
        self,
    ) -> None:
        selected = expand_test_selection(
            ("tests.test_beets_harness_session",),
            changed_paths=(),
            repo_root=REPO_ROOT,
        )

        ambient = ambient_test_modules(REPO_ROOT)
        self.assertEqual(selected[0], "tests.test_beets_harness_session")
        self.assertIn("tests.test_beets_harness_session_generated", selected)
        self.assertTrue(set(ambient).issubset(selected))
        self.assertEqual(len(selected), len(set(selected)))

    def test_every_audit_module_is_discovered_without_a_hand_maintained_list(
        self,
    ) -> None:
        ambient = ambient_test_modules(REPO_ROOT)

        self.assertTrue(set(ALWAYS_AMBIENT_TESTS).issubset(ambient))
        self.assertIn("tests.test_classify_producer_audit", ambient)
        self.assertIn("tests.web.test_routes_world_audit", ambient)
        self.assertEqual(len(ambient), len(set(ambient)))

    def test_generated_selection_adds_its_deterministic_sibling(self) -> None:
        selected = expand_test_selection(
            ("tests.test_beets_harness_session_generated",),
            changed_paths=(),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_beets_harness_session", selected)

    def test_changed_test_and_production_module_add_direct_neighbours(self) -> None:
        selected = expand_test_selection(
            (),
            changed_paths=(
                "tests/test_pyright_checks.py",
                "lib/artist_releases.py",
            ),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_pyright_checks", selected)
        self.assertNotIn("tests.test_pyright_checks_generated", selected)
        self.assertIn("tests.test_artist_releases", selected)

    def test_substrate_change_selects_the_modules_that_drive_its_behaviour(
        self,
    ) -> None:
        """Issue #1278 item 6: `scripts/test_substrate.py` owns the admission
        lock, headroom floors, `/proc` liveness, both reapers and the final
        gate itself, but its basename resolves only
        `tests.test_test_substrate` -- which pins the stdlib-only import
        boundary and nothing else. Without the EXACT_PATH_NEIGHBOURS entry,
        editing the reaper (or the gate's status ladder) would select no
        test that runs it, and the scripts-selection coverage audit of the
        day still passed: it required only one neighbour, which the
        basename candidate satisfied trivially. That gap is what item 9's
        `MASKABLE_ENTRY_PINS` closed -- this exact entry is pinned there
        now, and deleting it goes RED in three places in
        tests/test_selection_coverage_audit.py as well as here.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("scripts/test_substrate.py",),
            repo_root=REPO_ROOT,
        )

        self.assertTrue(
            {
                "tests.test_final_gate_receipt",
                "tests.test_suite_coordinator",
                "tests.test_test_tmpfs",
            }.issubset(selected),
            selected,
        )

    def test_final_gate_wrapper_change_selects_the_gate_contract_tests(
        self,
    ) -> None:
        """Before its EXACT_PATH_NEIGHBOURS entry, deleting the one `exec`
        line that reaches the real gate selected nothing at all, and no
        coverage audit noticed -- the scripts/ registry policed only
        `scripts/**/*.py` until issue #1278 item 9 put `.sh` on the same
        rule. The basename probe item 9 added does not cover this file
        either: there is no tests/test_run_final_gate.py, so the entry is
        still what selects the gate's real contract tests.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("scripts/run_final_gate.sh",),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_final_gate_receipt", selected)

    def test_enqueue_change_selects_its_behaviour_contracts(self) -> None:
        """Issue #1306: keep enqueue's narrow direct neighbour set exact."""
        self.assertEqual(
            _changed_path_neighbours("lib/enqueue.py", REPO_ROOT),
            (
                "tests.test_enqueue_fanout",
                "tests.test_enqueue_admission_generated",
                "tests.test_multidisc_manifest_generated",
                "tests.test_cross_request_enqueue_guard_generated",
            ),
        )

    def test_unmapped_shell_wrapper_fails_closed_with_its_name(self) -> None:
        """Issue #1278 item 9: a `scripts/**/*.sh` wrapper that resolves no
        neighbour must fail closed exactly as its `.py` siblings do. Until
        the scripts/ root rule policed `.sh`, thirteen of the sixteen real
        wrappers selected nothing at all and nothing said so.

        The probe path need not exist on disk -- `_changed_path_neighbours`
        never stats its own target, only candidate test modules.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"scripts/_totally_unmapped_probe\.sh",
        ):
            _changed_path_neighbours(
                "scripts/_totally_unmapped_probe.sh", REPO_ROOT
            )

    def test_shell_wrapper_resolves_its_basename_test_module(self) -> None:
        """The `.sh` basename probe is the same mechanical convention the
        `.py` roots already had: `scripts/fuzz_burst.sh` resolves
        `tests.test_fuzz_burst` with no hand-written entry of its own.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("scripts/fuzz_burst.sh",),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_fuzz_burst", selected)

    def test_pipeline_db_change_adds_shared_boundary_contracts(self) -> None:
        selected = expand_test_selection(
            (),
            changed_paths=("lib/pipeline_db/_requests.py",),
            repo_root=REPO_ROOT,
        )

        self.assertTrue(
            {
                "tests.test_pipeline_db",
                "tests.test_fakes",
                "tests.test_pipeline_db_write_audit",
                "tests.test_read_projection_audit",
                "tests.test_pipeline_db_column_contract",
            }.issubset(selected)
        )

    def test_route_and_nix_changes_add_their_structural_neighbours(self) -> None:
        selected = expand_test_selection(
            (),
            changed_paths=("web/routes/youtube.py", "nix/module.nix"),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.web.test_routes_youtube", selected)
        self.assertIn("tests.web.test_route_audit", selected)
        self.assertIn("tests.test_pydantic_route_audit", selected)
        self.assertIn("tests.test_js_payload_contract_audit", selected)
        self.assertIn("tests.test_nix_module", selected)

    def test_dispatch_entry_points_change_adds_its_real_consumers(self) -> None:
        """Issue #1196 item 4: ``lib/dispatch/entry_points.py`` had no
        ``EXACT_PATH_NEIGHBOURS`` entry. ``_direct_test_candidates`` only
        ever probes for ``tests.test_entry_points`` (which does not exist,
        since it derives from the basename, not the full path) — and
        because the file lives under ``lib/``, not ``tests/``,
        ``_changed_path_neighbours``'s fail-closed check (which only fires
        for unmapped ``tests/`` modules) never catches the resulting
        under-selection either. A diff touching only this file previously
        selected zero of its real behavior coverage.

        The original six-module set is qualified by fault injection (a
        ``raise RuntimeError`` planted as the first statement of
        ``dispatch_import_from_db``, run against each module — see the
        registry's own comment). ``tests.test_force_import_gates`` is
        deliberately absent: an #1196 review round found it kills nothing
        (its only references to this module are docstring lines saying
        coverage MOVED OUT after the U4 importer-never-measures refactor).

        Issue #1246 item 1 added two more: PR #1245's own
        ``tests.test_dispatch_outcomes_generated::TestGeneratedLaneDistanceAudit``
        was written specifically to patrol this file's lane discriminator
        but was never selected by a solo edit to it, and
        ``tests.test_local_import_lane`` pins the caller-side contract that
        discriminator depends on. Qualified the same way: flipping the
        discriminator's ``is not None`` to ``is None`` kills
        ``TestGeneratedLaneDistanceAudit`` (real dynamic execution); it does
        NOT kill ``test_local_import_lane``, which is kept anyway as a
        different, real regression class (see the registry's own comment).
        """
        selected = expand_test_selection(
            (),
            changed_paths=("lib/dispatch/entry_points.py",),
            repo_root=REPO_ROOT,
        )

        self.assertTrue(
            {
                "tests.test_dispatch_from_db",
                "tests.test_force_import_merge_redirect",
                "tests.test_integration_slices",
                "tests.test_import_manifest",
                "tests.test_import_queue",
                "tests.test_issue_573_boundaries",
                "tests.test_dispatch_outcomes_generated",
                "tests.test_local_import_lane",
            }.issubset(selected)
        )
        self.assertNotIn("tests.test_force_import_gates", selected)

    def test_importer_and_preview_worker_changes_add_their_real_consumers(
        self,
    ) -> None:
        """``scripts/importer.py`` and ``scripts/import_preview_worker.py``
        had no ``EXACT_PATH_NEIGHBOURS`` entry. Both live under ``scripts/``,
        not ``lib/``, so ``_changed_path_neighbours``'s lib-only fail-closed
        check never caught the resulting under-selection either -- silent,
        not admitted. A diff touching only ``scripts/importer.py``
        previously selected zero of its real behavior coverage, including
        ``TestCleanupTerminalForceActionFailsClosed`` in
        ``tests.test_import_queue`` -- and, sharper still, never selected
        ``tests.test_dispatch_outcomes_generated``, the generated property
        specifically written to patrol the lane discriminator this file's
        caller (``lib/dispatch/entry_points.py``) relies on -- the exact
        module whose non-selection is the reason this registry exists.

        Both sets are qualified by fault injection (a ``raise RuntimeError``
        planted as the first statement of each file's own central,
        every-job-type entry point -- ``process_claimed_job`` for
        importer.py, ``process_claimed_preview_job`` for import_preview_
        worker.py) run against every module found by grepping for real
        imports PLUS every module reaching the entry point indirectly
        through ``tests/dispatch_helpers.py::finalize_claimed_dispatch`` -- a
        QUALIFIED SUBSET of confirmed killers, not a claimed-complete kill
        set; see the registry's own comment for the full killed/not-killed/
        excluded-for-cost breakdown.
        """
        importer_selected = expand_test_selection(
            (),
            changed_paths=("scripts/importer.py",),
            repo_root=REPO_ROOT,
        )
        self.assertTrue(
            {
                "tests.test_import_dispatch",
                "tests.test_import_operation_fence",
                "tests.test_import_queue",
                "tests.test_integration_slices",
                "tests.test_local_import_lane",
                "tests.test_terminal_outcomes",
                "tests.test_dispatch_outcomes_generated",
                "tests.test_force_import_service_generated",
                "tests.test_import_job_lifecycle_generated",
                "tests.test_processing_lifecycle_generated",
                "tests.test_spectral_attempt_audit_generated",
                "tests.test_wrong_match_post_commit_generated",
            }.issubset(importer_selected)
        )

        preview_selected = expand_test_selection(
            (),
            changed_paths=("scripts/import_preview_worker.py",),
            repo_root=REPO_ROOT,
        )
        self.assertTrue(
            {
                "tests.test_import_queue",
                "tests.test_integration_slices",
                "tests.test_issue_1030_postgres_slice",
                "tests.test_terminal_outcome_callers",
                "tests.test_evidence_generated",
                "tests.test_path_authority_generated",
                "tests.test_preview_failure_evidence_generated",
                "tests.test_spectral_attempt_audit_generated",
            }.issubset(preview_selected)
        )

    def test_unknown_explicit_selector_fails_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown test selector"):
            expand_test_selection(
                ("tests.test_does_not_exist",),
                changed_paths=(),
                repo_root=REPO_ROOT,
            )

    def test_changed_shared_infra_module_does_not_self_select_as_a_bad_target(
        self,
    ) -> None:
        """Regression pin for issue #1081 / PR #1075's exact failure.

        ``tests/fakes/pipeline_db/import_jobs.py`` is not a discoverable test
        module, so it must never yield itself
        (``tests.fakes.pipeline_db.import_jobs``) as a selector. The
        ``tests/fakes/`` prefix rule still rescues the change with the real
        consumer, ``tests.test_fakes``.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("tests/fakes/pipeline_db/import_jobs.py",),
            repo_root=REPO_ROOT,
        )

        self.assertNotIn("tests.fakes.pipeline_db.import_jobs", selected)
        self.assertIn("tests.test_fakes", selected)

    def test_every_shared_tests_module_yields_an_accepted_selector(self) -> None:
        """Tree-walking pin: every non-test module under tests/ must map.

        Walks tests/ (not hard-listed) so a newly added shared module fails
        this test until scripts/targeted_test_selection.py maps it — the
        self-maintaining contract issue #1081 requires. The walk is rooted
        at REPO_ROOT/"tests" itself (never REPO_ROOT), so `.claude/worktrees/`
        stale checkouts — siblings of tests/, not descendants — are
        structurally unreachable; no runtime filter is needed to prove it
        (see #520/#543 for the prior REPO_ROOT-walk incidents that DID walk
        from REPO_ROOT and needed one).

        Uses complete_test_modules(), the same set main() resolves against —
        not discover_test_modules() alone — because
        tests.world_model.state_machine is deliberately excluded from the
        test*.py discovery glob and added back only by complete_test_modules().
        A bare discover_test_modules() set would make this pin reject the one
        honest neighbour that actually exercises tests/world_model/support.py.
        """
        modules = complete_test_modules(
            discover_test_modules(REPO_ROOT / "tests", REPO_ROOT, _DISCOVERY_PATTERN),
            REPO_ROOT,
        )
        tests_root = REPO_ROOT / "tests"
        shared_paths = sorted(
            path for path in tests_root.rglob("*.py") if not path.name.startswith("test")
        )
        self.assertTrue(shared_paths, "expected shared tests/ modules to exist")

        for path in shared_paths:
            relative = path.relative_to(REPO_ROOT).as_posix()
            with self.subTest(path=relative):
                if relative in SHARED_MODULES_WITHOUT_COVERAGE:
                    # Pins _changed_path_neighbours's early-return behavior
                    # for a registered gap: it selects nothing beyond
                    # ambient. This condition is identical to the
                    # early-return's own guard, so on its own it cannot
                    # prove the registry entry lacks a contradicting
                    # EXACT_PATH_NEIGHBOURS mapping for the same path — a
                    # prior version of this branch claimed it did (#1081
                    # review round 3), which was a tautology. That
                    # contradiction is instead made impossible at import
                    # time by _assert_no_double_registration, self-tested in
                    # test_double_registered_path_fails_at_import_time below.
                    self.assertEqual(
                        _changed_path_neighbours(relative, REPO_ROOT),
                        (),
                    )
                    continue
                selection = expand_test_selection(
                    (),
                    changed_paths=(relative,),
                    repo_root=REPO_ROOT,
                )
                # hotspot_policies={} / hotspot_isolated_methods={}: this pin
                # only proves every selector resolves to a real discovered
                # module (issue #1081's actual concern) — hotspot
                # method/class sharding AND the cost-aware isolation
                # carve-out (issue #1131 review round 2) are a separate,
                # already-covered concern (tests.test_parallel_test_runner)
                # that needs an expensive discovery-manifest subprocess per
                # hotspot module, irrelevant here. Passing only
                # hotspot_policies={} is NOT enough on its own:
                # select_test_targets defaults hotspot_isolated_methods to
                # the real HOTSPOT_ISOLATED_METHODS registry, so a selector
                # resolving to tests.test_nix_module would still route
                # through hotspot_targets (isolation is non-empty even
                # with an empty shard policy) and raise "missing discovery
                # manifest" here, since no listed_test_ids was supplied.
                select_test_targets(
                    modules,
                    selection,
                    hotspot_policies={},
                    hotspot_isolated_methods={},
                )

    def test_shared_fakes_map_to_their_real_consumers_not_only_test_fakes(
        self,
    ) -> None:
        """Regression pin for issue #1081 review round 2, MUST FIX 2.

        The tests/fakes/ prefix rule alone maps every fake to
        tests.test_fakes, but five fakes are neither imported by
        tests/test_fakes.py nor re-exported by tests/fakes/__init__.py —
        tests.test_fakes never loads them. tests/fakes/deploy_hold.py is the
        live instance: another agent was editing it while this regression
        shipped, and the prefix rule alone would have selected a test that
        never loads it.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("tests/fakes/deploy_hold.py",),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_fakes", selected)
        self.assertIn("tests.test_deploy_hold", selected)
        self.assertIn("tests.test_deploy_hold_generated", selected)

    def test_the_two_registries_are_disjoint(self) -> None:
        """No path claims both a real mapping and an admitted coverage gap.

        Defensive restatement of the guard that already ran at module import
        (_assert_no_double_registration, called with the real registries at
        the bottom of scripts/targeted_test_selection.py) — if this were
        ever violated, the module would already have failed to import before
        this test file could even load. Named here so the failure mode has a
        home in the test suite, not only a traceback at collection time.
        """
        self.assertEqual(
            set(EXACT_PATH_NEIGHBOURS) & set(SHARED_MODULES_WITHOUT_COVERAGE),
            set(),
        )

    def test_double_registered_path_fails_at_import_time(self) -> None:
        """Known-bad self-test for _assert_no_double_registration.

        A path present in both registries would silently discard its real
        EXACT_PATH_NEIGHBOURS mapping — _changed_path_neighbours returns
        early for any SHARED_MODULES_WITHOUT_COVERAGE path before
        EXACT_PATH_NEIGHBOURS is even consulted (#1081 review round 3). This
        proves the checker actually trips on a planted contradiction, using
        synthetic registries so the test does not depend on (or risk
        corrupting) the real module-level dicts.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"tests/_double_registered\.py",
        ):
            _assert_no_double_registration(
                {"tests/_double_registered.py": ("tests.test_something",)},
                {"tests/_double_registered.py": "claimed as both mapped and a gap"},
            )

    def test_exact_path_neighbour_keys_still_exist_on_disk(self) -> None:
        """Reverse direction of the tree-walking pin: no stale mapping keys.

        A file deleted or renamed out from under an EXACT_PATH_NEIGHBOURS /
        SHARED_MODULES_WITHOUT_COVERAGE entry leaves a dead key that nothing
        else catches — the forward walk only ever visits real files.
        """
        for key in (*EXACT_PATH_NEIGHBOURS, *SHARED_MODULES_WITHOUT_COVERAGE):
            with self.subTest(key=key):
                self.assertTrue(
                    (REPO_ROOT / key).is_file(),
                    f"mapping key does not exist on disk: {key}",
                )

    def test_unmapped_shared_test_module_fails_closed_with_the_file_name(
        self,
    ) -> None:
        """Known-bad self-test: an unmapped shared module must fail loudly.

        Silent dropping under-selects; a raw ``ValueError`` from deep inside
        ``select_test_targets`` would not name the offending file. The
        domain-level check in ``_changed_path_neighbours`` must raise before
        that, naming the exact path.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"tests/_totally_unmapped_shared_helper\.py",
        ):
            expand_test_selection(
                (),
                changed_paths=("tests/_totally_unmapped_shared_helper.py",),
                repo_root=REPO_ROOT,
            )

    def test_selection_checker_names_missing_and_duplicate_targets(self) -> None:
        with self.assertRaisesRegex(AssertionError, "missing.*tests.test_beta"):
            assert_selection_complete(
                ("tests.test_alpha",),
                ("tests.test_alpha", "tests.test_beta"),
            )
        with self.assertRaisesRegex(AssertionError, "duplicate.*tests.test_alpha"):
            assert_selection_complete(
                ("tests.test_alpha", "tests.test_alpha"),
                ("tests.test_alpha",),
            )


class TestTargetedSuiteWiring(unittest.TestCase):
    def test_targeted_suite_reuses_every_non_python_canonical_phase(self) -> None:
        phases = targeted_phases(("tests.test_pyright_checks",))

        self.assertEqual(
            tuple(phase.name for phase in phases),
            ("js-syntax", "js-unit", "pyright", "ruff", "vulture", "python"),
        )
        python_phase = phases[-1]
        self.assertEqual(python_phase.parser, "python")
        self.assertEqual(
            python_phase.command,
            (
                "python3",
                "scripts/run_python_tests.py",
                "--test",
                "tests.test_pyright_checks",
            ),
        )

    def test_shell_entrypoint_forwards_every_selector_to_targeted_runner(self) -> None:
        source = pinned_source(REPO_ROOT / "scripts" / "test.sh")

        self.assertIn("scripts/run_targeted_tests.py", source)
        self.assertIn('"$@"', source)
        self.assertNotIn("python3 -m unittest discover", source)

    def test_shell_entrypoint_sets_the_suite_owns_headroom_env_var(self) -> None:
        """Issue #1111 review MAJOR-3: the M2 producer side is otherwise
        unpinned — deleting `env CRATEDIGGER_SUITE_OWNS_HEADROOM=1` from
        scripts/test.sh's own dev-shell invocation would leave every other
        test green while M2 silently reverts to the old shell-entry-dies-
        under-contention shape.

        Issue #1229 moved the launcher from `nix-shell --run` to `nix
        develop --command` for Nix's flake eval cache; the var must be set
        on whichever one is there, so both the var AND the launcher it
        prefixes are pinned together."""
        source = pinned_source(REPO_ROOT / "scripts" / "test.sh")

        self.assertIn(
            "env CRATEDIGGER_SUITE_OWNS_HEADROOM=1 nix develop", source
        )

    def test_targeted_runner_takes_the_same_suite_admission_lock(self) -> None:
        """Issue #1111 review B1: scripts/test.sh targeted runs DO
        participate in run_suite's admission lock — they are NOT excluded
        the way an interactive nix-shell entry is. run_targeted_tests.py
        calls run_suite() with no runtime_dir override, so it resolves the
        SAME shared root and contends for the SAME lockfile as the canonical
        suite; #1111's own incident record includes a scripts/test.sh
        BrokenProcessPool collision, which is exactly what this admission
        gate exists to prevent.

        Proven end-to-end against the real entry point (not a reconstruction
        of what it "should" do): hold the lock in an isolated runtime dir,
        launch the real run_targeted_tests.py against that same dir via
        XDG_RUNTIME_DIR, and require its own "waiting for admission" message
        to name that exact lockfile before it is killed — a later change
        that silently routes targeted runs around admission fails this pin.
        """
        shared = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{os.getuid()}"))
        self.assertTrue(shared.is_dir(), "private runtime tmpfs is required")
        isolated = Path(
            # Matches _REAPABLE_PREFIXES' "cratedigger-admission-test-" entry
            # (scripts/test_substrate.py) — a literal-prefix glob, so a
            # differently-worded prefix here would silently escape reaping
            # (issue #1111 review m12).
            tempfile.mkdtemp(dir=shared, prefix="cratedigger-admission-test-")
        )
        lock_path = admission_lock_path(isolated)
        held = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        fcntl.flock(held, fcntl.LOCK_EX)
        process: subprocess.Popen[bytes] | None = None
        try:
            process = subprocess.Popen(
                [
                    sys.executable,
                    str(REPO_ROOT / "scripts" / "run_targeted_tests.py"),
                    "tests.test_typing_ratchet",
                ],
                cwd=REPO_ROOT,
                env={**os.environ, "XDG_RUNTIME_DIR": str(isolated)},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            assert process.stdout is not None
            fd = process.stdout.fileno()
            buffer = b""
            deadline = time.monotonic() + 30.0
            saw_waiting = False
            while time.monotonic() < deadline:
                remaining = max(0.0, deadline - time.monotonic())
                ready, _write, _err = select.select([fd], [], [], min(1.0, remaining))
                if not ready:
                    continue
                chunk = os.read(fd, 4096)
                if not chunk:
                    break
                buffer += chunk
                if b"waiting for admission" in buffer and str(lock_path).encode() in buffer:
                    saw_waiting = True
                    break

            self.assertTrue(
                saw_waiting,
                "run_targeted_tests.py never reported waiting for admission "
                f"on {lock_path}; captured output: {buffer.decode(errors='replace')}",
            )
        finally:
            if process is not None:
                process.kill()
                process.wait(timeout=10)
                if process.stdout is not None:
                    process.stdout.close()
            fcntl.flock(held, fcntl.LOCK_UN)
            os.close(held)
            shutil.rmtree(isolated, ignore_errors=True)

    def test_python_phase_completes_for_a_diff_touching_shared_fakes(self) -> None:
        """End-to-end pin for issue #1081: a real diff touching tests/fakes/*.py
        must not die on selector resolution — PR #1075's exact failure mode,
        where js/pyright/ruff/vulture all pass and the python phase exits 1
        before any test runs.

        CRATEDIGGER_TEST_JOBS=4 caps the nested runner's worker count.
        worker_environment() unconditionally pops TEST_DB_DSN so every
        persistent worker bootstraps its own ephemeral PostgreSQL — at the
        default worker count (half the host's CPUs, capped at 12) that is up
        to 12 nested clusters spun up inside one already-parallel outer suite
        target: the same class of scheduler-contention flake fixed by
        widening the sp.run timeout in
        tests/test_beets_destructive_configs_generated.py elsewhere in this
        PR. An earlier version of this pin pinned CRATEDIGGER_TEST_JOBS=1,
        which traded that speculative flake for a measured one: on this host,
        JOBS=1 measured 123.5s under 2x CPU oversubscription against a 180s
        bound (~1.45x headroom), while JOBS=4 measured 46.1s (comparable to
        the unbounded-worker 39.4s). JOBS=4 plus a 300s bound keeps this a
        real end-to-end subprocess run of the actual entrypoint with real
        margin in both directions, without the full nested-cluster count of
        an unbounded run.

        A failure here may duplicate an unrelated ambient audit's own
        failure reported elsewhere in the outer suite — expand_test_selection
        always appends every ambient audit, so this real subprocess reruns
        all of them too. That is the accepted cost of driving the real
        entrypoint rather than a hand-picked subset; the tail-truncated
        detail below keeps the duplicate report short.
        """
        selectors = expand_test_selection(
            (),
            changed_paths=("tests/fakes/pipeline_db/import_jobs.py",),
            repo_root=REPO_ROOT,
        )
        python_phase = targeted_phases(selectors)[-1]
        self.assertEqual(python_phase.name, "python")

        completed = subprocess.run(
            python_phase.command,
            cwd=REPO_ROOT,
            env={**os.environ, "CRATEDIGGER_TEST_JOBS": "4"},
            capture_output=True,
            text=True,
            timeout=300,
            check=False,
        )

        detail = (completed.stdout + completed.stderr)[-4000:]
        self.assertEqual(completed.returncode, 0, detail)


if __name__ == "__main__":
    unittest.main()
