"""Deterministic contracts for targeted plus adjacent test selection."""

from __future__ import annotations

import contextlib
import fcntl
import io
import os
import select
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path, PurePosixPath

from scripts.phase_parsers import python_tests
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
    BASENAME_RULES,
    EXACT_PATH_NEIGHBOURS,
    EXACT_TABLE_SOURCE,
    PIPELINE_DB_NEIGHBOURS,
    PREFIX_RULES,
    SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE,
    SELECTION_RULES,
    SELF_SELECTOR_SOURCE,
    SHARED_MODULES_WITHOUT_COVERAGE,
    SelectionRule,
    _assert_no_double_registration,
    _assert_selection_rules_well_formed,
    _basename_rule,
    _changed_path_neighbours,
    _direct_test_candidates,
    ambient_test_modules,
    assert_selection_complete,
    expand_test_selection,
    explain_path,
    resolve_attributed_neighbours,
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

    def test_a_fake_selects_its_own_cluster_test_module(self) -> None:
        """The #1313 split moved TestFakeBeetsDB out of tests/test_fakes.py.

        tests/test_fakes.py no longer names FakeBeetsDB anywhere, so the
        tests/fakes/ prefix rule alone would select a module that never
        loads the fake being edited: the same shape as the deploy_hold pin
        above. The derived tests.test_fakes_<stem> row is what still
        reaches the real consumer.
        """
        source = (REPO_ROOT / "tests" / "test_fakes.py").read_text()
        self.assertNotIn("FakeBeetsDB", source)

        selected = expand_test_selection(
            (),
            changed_paths=("tests/fakes/beets.py",),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_fakes_beets", selected)

    def test_a_pipeline_db_cluster_selects_its_mirrored_fake_tests(self) -> None:
        """One derived name serves both sides of the mirror (#1313).

        tests/fakes/pipeline_db/ mirrors lib/pipeline_db/ module for module,
        so editing either the production cluster or its fake selects that
        cluster's own fake self-tests.
        """
        for changed in (
            "lib/pipeline_db/transfer_ledger.py",
            "tests/fakes/pipeline_db/transfer_ledger.py",
        ):
            with self.subTest(changed=changed):
                selected = expand_test_selection(
                    (), changed_paths=(changed,), repo_root=REPO_ROOT,
                )
                self.assertIn("tests.test_fakes_transfer_ledger", selected)

    def test_a_cluster_without_sibling_tests_still_reaches_test_fakes(
        self,
    ) -> None:
        """The derived row adds precision; it is not the fail-closed floor.

        tests/fakes/pipeline_db/cleanup_journal.py has no
        tests/test_fakes_cleanup_journal.py, so the derived name resolves
        nothing and the change must still land on tests.test_fakes rather than
        on a module that does not exist. Any sibling-less cluster serves; the
        assertFalse below is what tells whoever gives this one its own module
        to repoint the example (issue #1313, where giving evidence a sibling
        did exactly that).
        """
        self.assertFalse(
            (REPO_ROOT / "tests" / "test_fakes_cleanup_journal.py").exists())

        selected = expand_test_selection(
            (),
            changed_paths=("tests/fakes/pipeline_db/cleanup_journal.py",),
            repo_root=REPO_ROOT,
        )

        self.assertIn("tests.test_fakes", selected)
        self.assertNotIn("tests.test_fakes_cleanup_journal", selected)

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


class TestSelectionRuleTable(unittest.TestCase):
    """`SELECTION_RULES` and the `explain` seam (issue #1313).

    The basename conventions and directory rules used to be hand-written
    `if` branches. As data they are auditable and attributable; these are
    the contracts that keep them honest. Selection machinery, so
    deterministic tests only.
    """

    #: Extra shapes for the disjointness pin. Only `probe.sh` genuinely
    #: extends it beyond `_repository_paths()` — the tree holds no top-level
    #: shell script. The rest (a `.sh` outside scripts/, a route module, a
    #: top-level `.py`) all exist under the walked roots already; they are
    #: kept because they are the shapes a reader checks first, and because a
    #: rule edit that breaks them should fail on a named probe rather than on
    #: whichever real file happens to match.
    SYNTHETIC_PROBES = (
        "lib/probe.sh",
        "scripts/probe.sh",
        "scripts/probe.py",
        "web/probe.py",
        "web/routes/probe.py",
        "probe.py",
        "probe.sh",
        "harness/probe.py",
        "tests/probe.py",
        "migrations/099_probe.sql",
    )

    #: Every root a basename rule can match, plus the top level. Rooted at
    #: each directory rather than REPO_ROOT, so `.claude/worktrees/` stale
    #: checkouts are structurally unreachable (the #520/#543 shape); the
    #: top-level sweep uses `iterdir`, which does not descend at all.
    WALKED_ROOTS = ("lib", "scripts", "web", "tests", "harness")

    def _repository_paths(self) -> list[str]:
        """Every real file a basename rule could match, walked not queried.

        Deliberately not `git ls-files`: this test has to keep working
        inside a copied tree that is not a git worktree, which is where
        mutmut runs it (issue #1325 residual 5 recorded that shape for this
        very file, and a `git ls-files` version of this pin reproduced it —
        every path came back empty and the sweep silently covered nothing).
        """
        paths = [
            path.relative_to(REPO_ROOT).as_posix()
            for root in self.WALKED_ROOTS
            for path in (REPO_ROOT / root).rglob("*")
            if path.is_file() and "__pycache__" not in path.parts
        ]
        paths.extend(
            path.name for path in REPO_ROOT.iterdir() if path.is_file()
        )
        return paths

    def test_at_most_one_basename_rule_matches_any_path(self) -> None:
        """`_basename_rule` speaks of "the" matching rule, so there must be
        at most one.

        The rows are meant to be disjoint by construction — different roots,
        different suffixes within a root, and a top-level rule no rooted path
        can reach. If two ever overlapped, the first would silently win and
        the second's templates would vanish with nothing saying so. Checked
        over every real file under a root a basename rule can reach, plus
        shapes the tree does not currently hold.
        """
        walked = self._repository_paths()
        self.assertGreater(len(walked), 100, "expected a populated worktree")

        for relative in (*walked, *self.SYNTHETIC_PROBES):
            path = PurePosixPath(relative)
            matching = [
                rule.name
                for rule in BASENAME_RULES
                if rule.matches(relative, path)
            ]
            with self.subTest(path=relative):
                self.assertLessEqual(len(matching), 1, matching)

    def test_duplicate_rule_names_are_refused_at_import_time(self) -> None:
        """Known-bad self-test. Two rows sharing a name make `explain`'s
        attribution ambiguous and let one row's pin stand in for another's.
        """
        row = SelectionRule(
            name="probe", description="probe", root="lib", derived=("tests.x",)
        )
        with self.assertRaisesRegex(
            ValueError, r"duplicate SelectionRule name: probe"
        ):
            _assert_selection_rules_well_formed((row, row))

    def test_a_rule_matching_every_path_is_refused_at_import_time(self) -> None:
        """Known-bad self-test. A row with no path condition matches every
        file in the repository, so its modules would join every selection.
        `suffixes` alone is not a path condition — `.py` is most of the tree.
        """
        with self.assertRaisesRegex(
            ValueError,
            r"constrains no path and would match every file in the repository",
        ):
            _assert_selection_rules_well_formed(
                (
                    SelectionRule(
                        name="probe",
                        description="probe",
                        suffixes=(".py",),
                        neighbours=("tests.test_fakes",),
                    ),
                )
            )

    def test_a_rule_contributing_nothing_is_refused_at_import_time(self) -> None:
        """Known-bad self-test. A row that matches paths and names no module
        reads as coverage in the table while selecting nothing.
        """
        with self.assertRaisesRegex(ValueError, r"contributes no test module"):
            _assert_selection_rules_well_formed(
                (
                    SelectionRule(
                        name="probe", description="probe", prefixes=("lib/",)
                    ),
                )
            )

    def test_a_duplicate_rule_description_is_refused_at_import_time(
        self,
    ) -> None:
        """Known-bad self-test. `explain` prints a description beside every
        attributed module, so a row wearing a neighbour's sentence tells an
        operator that a route file matched the rule for non-route files.
        The review round planted exactly that and nothing failed.
        """
        shared = "the same sentence on two different rules"
        with self.assertRaisesRegex(
            ValueError, r"duplicate SelectionRule description on second"
        ):
            _assert_selection_rules_well_formed(
                (
                    SelectionRule(
                        name="first",
                        description=shared,
                        root="lib",
                        derived=("tests.test_{stem}",),
                    ),
                    SelectionRule(
                        name="second",
                        description=shared,
                        root="web",
                        derived=("tests.test_{stem}",),
                    ),
                )
            )

    def test_the_real_table_passes_its_own_checker(self) -> None:
        """Must-still-work control for the four clauses above."""
        _assert_selection_rules_well_formed(SELECTION_RULES)
        self.assertEqual(SELECTION_RULES, (*BASENAME_RULES, *PREFIX_RULES))

    def test_the_module_runs_its_own_checker_at_import(self) -> None:
        """The four clauses above call the checker directly, so none of them
        notices if the module stops calling it — deleting the import-time
        call left all of them green. Pinned as source because that call is a
        module-level statement no in-process test can re-trigger.
        """
        source = pinned_source(
            REPO_ROOT / "scripts" / "targeted_test_selection.py"
        )

        self.assertIn(
            "_assert_selection_rules_well_formed(SELECTION_RULES)", source
        )

    def test_the_rule_tables_are_a_seam_the_resolver_really_reads(self) -> None:
        """Both table kwargs reach the resolution AND the fail-closed raise.

        `tests/test_selection_coverage_audit.py`'s contract D measures a
        row's deletion visibility by handing the resolver the table minus
        that row; a resolver that quietly read the module globals instead
        would report every row as safe. Three paths, one per stage: a route
        module has nothing but its prefix rule, `lib/download.py` keeps its
        hand-authored entry when the basename stage is empty, and a file
        that resolves through the rules alone fails closed with both tables
        emptied.
        """
        route = "web/routes/pipeline.py"
        self.assertEqual(
            resolve_attributed_neighbours(
                route, PurePosixPath(route), REPO_ROOT, prefix_rules=()
            ),
            (),
        )

        download = "lib/download.py"
        self.assertEqual(
            [
                source.name
                for source in resolve_attributed_neighbours(
                    download,
                    PurePosixPath(download),
                    REPO_ROOT,
                    basename_rules=(),
                )
            ],
            [EXACT_TABLE_SOURCE],
        )

        with self.assertRaisesRegex(
            ValueError, r"unmapped lib module: lib/quality/verdict_tiers\.py"
        ):
            _changed_path_neighbours(
                "lib/quality/verdict_tiers.py",
                REPO_ROOT,
                basename_rules=(),
                prefix_rules=(),
            )

        # A SUBSTITUTE row, not an empty table. Removing rows cannot tell
        # whether the basename stage really reads the passed table: the
        # rows are disjoint, so a stage that ignored the kwarg would still
        # match the same row whenever the outer lookup found one at all,
        # and two survivors said so (the `basename_rules=` forward dropped
        # at `_direct_test_candidates`' own call to `_basename_rule`, and
        # at this function's call to `_direct_test_candidates`). Only a
        # table naming a DIFFERENT row for the same path separates them.
        substitute = SelectionRule(
            name="basename:_substitute_probe",
            description="a substitute basename row, for this pin only",
            root="lib",
            suffixes=(".py",),
            derived=("tests.test_targeted_test_selection",),
        )
        substituted = [
            (source.name, source.modules)
            for source in resolve_attributed_neighbours(
                download,
                PurePosixPath(download),
                REPO_ROOT,
                basename_rules=(substitute,),
            )
            if source.name == substitute.name
        ]

        self.assertEqual(
            substituted,
            [(substitute.name, ("tests.test_targeted_test_selection",))],
        )
        self.assertEqual(
            _direct_test_candidates(
                PurePosixPath(download), basename_rules=(substitute,)
            ),
            ("tests.test_targeted_test_selection",),
        )

    def test_attribution_names_the_mechanism_behind_every_module(self) -> None:
        """Three mechanisms fire for one path, and each names what it added.

        `lib/pipeline_db/decisions.py` is the richest real instance: a
        hand-authored entry, a basename rule that finds nothing on disk, and
        the pipeline-DB prefix rule. Before this table, "which rule selected
        this module" meant reading the file or diffing a run.
        """
        relative = "lib/pipeline_db/decisions.py"
        sources = resolve_attributed_neighbours(
            relative, PurePosixPath(relative), REPO_ROOT
        )

        self.assertEqual(
            [source.name for source in sources],
            [
                EXACT_TABLE_SOURCE,
                "basename:lib/*.py",
                "prefix:lib/pipeline_db/",
            ],
        )
        # Every source carries the sentence `explain` prints beside it. The
        # two that belong to no rule are spelled here because nothing else
        # holds them; the rule-owned two come from the table, so a row whose
        # description drifts from what it does is caught at its own site.
        # Spelled out, not read back from the table: `explain` prints these
        # beside every attributed module, and comparing a row against itself
        # passes for any sentence at all. The review round proved that by
        # giving one row a neighbour's description and watching every test
        # stay green. Changing a description is now a deliberate two-place
        # edit, the same boundary MASKABLE_ENTRY_PINS has.
        self.assertEqual(
            [source.description for source in sources],
            [
                "hand-authored entry for this exact path",
                (
                    "a lib module is covered by tests/test_<stem>.py and its "
                    "generated sibling"
                ),
                (
                    "a production DB cluster regresses the shared boundary "
                    "contracts and its own mirrored fake's self-tests"
                ),
            ],
        )
        self.assertEqual(
            sources[0].modules, EXACT_PATH_NEIGHBOURS[relative]
        )
        # The basename rule matched and contributed nothing: neither
        # tests.test_decisions nor its generated sibling exists, which is
        # exactly why this path carries a hand-authored entry.
        self.assertEqual(sources[1].modules, ())
        self.assertEqual(
            sources[1].unresolved,
            ("tests.test_decisions", "tests.test_decisions_generated"),
        )
        self.assertEqual(sources[2].modules, PIPELINE_DB_NEIGHBOURS)
        self.assertEqual(
            sources[2].unresolved, ("tests.test_fakes_decisions",)
        )

    def test_the_hand_authored_entry_resolves_before_the_prefix_rules(
        self,
    ) -> None:
        """Order is a contract, not an accident: the runner takes the
        selection in this order, and the whole table exists to preserve it.
        Built from the named constants, so it cannot rot into a restatement
        of whatever the resolver happens to do.
        """
        relative = "lib/pipeline_db/decisions.py"

        self.assertEqual(
            _changed_path_neighbours(relative, REPO_ROOT),
            (*EXACT_PATH_NEIGHBOURS[relative], *PIPELINE_DB_NEIGHBOURS),
        )

    def test_a_changed_test_module_is_attributed_to_the_self_selector(
        self,
    ) -> None:
        """The one mechanism that is neither a table entry nor a rule."""
        relative = "tests/test_pyright_checks.py"
        sources = resolve_attributed_neighbours(
            relative, PurePosixPath(relative), REPO_ROOT
        )

        self.assertEqual([source.name for source in sources], [SELF_SELECTOR_SOURCE])
        self.assertEqual(sources[0].modules, ("tests.test_pyright_checks",))
        self.assertEqual(
            sources[0].description,
            "the changed file is itself a runnable test module",
        )

    def test_explain_names_the_rule_that_selected_each_module(self) -> None:
        """Asserted as LINES, not as substrings of the joined report.

        A substring assertion here is satisfied by the flat `selects:`
        summary at the bottom, so deleting the per-source module breakdown
        entirely left this test green — it was not constraining the
        attribution it is named for (review round, mutant M18).
        """
        report = explain_path("web/routes/youtube.py", REPO_ROOT)

        rule_line = next(
            line for line in report if line.startswith("  prefix:web/routes/")
        )
        self.assertIn("a route regresses the route audits", rule_line)
        self.assertIn("      tests.web.test_routes_youtube", report)
        self.assertIn("      tests.web.test_route_audit", report)
        # The whole sentence, as a line: `web/` is policed by no root rule,
        # and a reader who only sees "policed by: nothing" does not learn
        # that zero neighbours here would be silent rather than fatal.
        self.assertIn(
            "  policed by: nothing — no ROOT_COVERAGE_RULES row covers this "
            "root and suffix, so resolving zero neighbours here is silent",
            report,
        )

    def test_a_route_module_never_matches_the_web_basename_rule(self) -> None:
        """`excluded_prefixes` was the one `matches` clause with no test.

        Neutralising it changes no selection today, only because no route
        stem has a `tests/test_web_<stem>.py` or `tests/web/test_<stem>.py`
        to resolve — so the loss is silent until one appears. What it does
        change immediately is what `explain` says: a route file attributed
        to the rule for NON-route modules, with two phantom missing names
        under it.
        """
        route = PurePosixPath("web/routes/youtube.py")

        self.assertIsNone(_basename_rule(route))
        self.assertEqual(_direct_test_candidates(route), ())

        report = explain_path("web/routes/youtube.py", REPO_ROOT)
        self.assertEqual(
            [line for line in report if line.startswith("  basename:")], []
        )

    def test_explain_says_so_when_no_mechanism_matches_at_all(self) -> None:
        """A path outside every root and every rule. The line is the whole
        answer for it, so it is asserted as a line, not as a substring.
        """
        report = explain_path("docs/mirrors.md", REPO_ROOT)

        self.assertIn("  (no mechanism matched this path)", report)
        self.assertNotIn(
            "  (no mechanism matched this path)",
            explain_path("lib/download.py", REPO_ROOT),
        )

    def test_explain_reports_a_derived_name_with_no_module_on_disk(self) -> None:
        """The single most common reason a path resolves less than expected,
        and the reason `NeighbourSource` carries `unresolved` at all.

        Any sibling-less cluster serves as the example; the assertFalse tells
        whoever gives this one a module to repoint it (issue #1313).
        """
        self.assertFalse(
            (REPO_ROOT / "tests" / "test_fakes_cleanup_journal.py").exists())

        report = "\n".join(
            explain_path("tests/fakes/pipeline_db/cleanup_journal.py", REPO_ROOT)
        )

        self.assertIn(
            "tests.test_fakes_cleanup_journal  (no module file on disk)", report)
        self.assertIn("selects: tests.test_fakes", report)

    def test_explain_reports_a_fail_closed_path_instead_of_raising(self) -> None:
        """A diagnostic that dies on the paths worth diagnosing is useless:
        an unmapped file is exactly when someone runs `explain`.
        """
        report = "\n".join(
            explain_path("lib/_no_such_module_anywhere.py", REPO_ROOT)
        )

        self.assertIn("fails closed", report)
        self.assertIn("unmapped lib module", report)
        self.assertIn("lib/_no_such_module_anywhere.py", report)

    def test_explain_reports_an_admitted_gap_with_its_rationale(self) -> None:
        registered = next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))
        rationale = SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE[registered]

        report = "\n".join(explain_path(registered, REPO_ROOT))

        self.assertIn("SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE", report)
        self.assertIn(rationale, report)
        self.assertIn("selects: nothing beyond the ambient gates", report)

    def test_explain_reports_the_paired_sibling_no_rule_contributes(
        self,
    ) -> None:
        """`expand_test_selection` adds a deterministic/generated sibling on
        top of the neighbours, and no rule contributes it.

        Reporting only the neighbours under-stated what really runs on 254
        of 1,619 tracked paths. `migrations/*.sql` is the clearest case: the
        prefix rule names `tests.test_migrator`, and `tests.test_migrator_
        generated` runs too.
        """
        report = explain_path("migrations/001_initial.sql", REPO_ROOT)
        selects = next(line for line in report if "  selects: " in line)

        self.assertIn(
            "  paired siblings — added by expand_test_selection, not by any "
            "rule:",
            report,
        )
        self.assertIn("      tests.test_migrator_generated", report)
        self.assertIn("tests.test_migrator_generated", selects)
        # The third thing that runs and no rule names. Asserted as a line
        # because it is the sentence that stops a reader treating `selects:`
        # as the complete list.
        self.assertIn(
            "      plus every ambient audit and ratchet, which run on every "
            "selection regardless of the path",
            report,
        )
        # And a path with no sibling to add says nothing about pairing.
        self.assertEqual(
            [
                line
                for line in explain_path("web/routes/youtube.py", REPO_ROOT)
                if "paired siblings" in line
            ],
            [],
        )

    def test_explain_says_an_early_returning_gap_discards_everything(
        self,
    ) -> None:
        """The `tests/` registry returns before resolution, so a registered
        path selects nothing even though a prefix rule would have named six
        modules. Showing that discarded set with no explanation is the one
        place the report can read as self-contradictory.
        """
        registered = "tests/world_model/mirror_harness.py"
        self.assertIn(registered, SHARED_MODULES_WITHOUT_COVERAGE)

        report = explain_path(registered, REPO_ROOT)

        self.assertIn("      tests.test_world_model_burst", report)
        self.assertIn(
            "      this registry selects NOTHING at all: every module above "
            "is discarded before resolution, so a registration cannot be a "
            "lookalike neighbour set (issue #1081)",
            report,
        )
        self.assertIn(
            "  selects: nothing beyond the ambient gates", report
        )

    def test_explain_swallows_the_resolver_stderr_note_it_duplicates(
        self,
    ) -> None:
        """`_changed_path_neighbours` writes the admitted-gap note to stderr
        every time it resolves one. The report already states the gap and its
        full rationale, so the duplicate is suppressed — a claim the
        docstring makes and nothing else checks: `redirect_stderr(None)`
        sends the note to stdout instead, where the report's own assertions
        cannot see it.
        """
        registered = next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))
        out, err = io.StringIO(), io.StringIO()

        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            explain_path(registered, REPO_ROOT)

        self.assertEqual(out.getvalue(), "")
        self.assertEqual(err.getvalue(), "")

    def test_explain_deduplicates_what_it_reports_as_selected(self) -> None:
        """`lib/download.py` resolves tests.test_download twice — its entry
        and its basename rule both name it. The runner sees one target, so
        the summary line says one, while the breakdown above still shows both
        mechanisms naming it.

        Parsed into a list rather than counted as a substring: the duplicate
        lands LAST, so `count("tests.test_download,")` stays 1 whether or not
        the line is de-duplicated, and a mutant dropping `_ordered_unique`
        survived that spelling.
        """
        report = explain_path("lib/download.py", REPO_ROOT)
        selects = next(line for line in report if "  selects: " in line)
        listed = [name.strip() for name in selects.split(": ", 1)[1].split(",")]

        self.assertIn("tests.test_download", listed)
        self.assertEqual(sorted(listed), sorted(set(listed)), listed)
        self.assertEqual(
            "\n".join(report).count("      tests.test_download\n"), 2
        )

    def test_the_explain_entry_point_runs_as_a_script(self) -> None:
        """Drives the real entry point, not `main()` in-process: the module
        is documented as runnable, and the `__main__` guard plus argparse
        wiring is what an operator actually meets.
        """
        completed = subprocess.run(
            [
                sys.executable,
                str(REPO_ROOT / "scripts" / "targeted_test_selection.py"),
                "explain",
                "lib/download.py",
                "docs/mirrors.md",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("basename:lib/*.py", completed.stdout)
        self.assertIn("(no mechanism matched this path)", completed.stdout)

    def test_the_explain_entry_point_refuses_a_bad_invocation(self) -> None:
        """Both refusals, because they are enforced by different things.

        An unknown subcommand is rejected by argparse's `choices` whatever
        `required=` says; NO subcommand is rejected only by `required=True`.
        Covering the first alone left flipping that flag green, and the
        no-argv run then died with an uncaught `AttributeError` instead of a
        usage message (review round, mutant M35).
        """
        script = str(REPO_ROOT / "scripts" / "targeted_test_selection.py")
        for argv, case in (
            (["resolve", "lib/download.py"], "unknown subcommand"),
            ([], "no subcommand at all"),
        ):
            with self.subTest(case=case):
                completed = subprocess.run(
                    [sys.executable, script, *argv],
                    cwd=REPO_ROOT,
                    capture_output=True,
                    text=True,
                    check=False,
                )

                self.assertEqual(completed.returncode, 2, completed.stdout)
                self.assertIn("usage:", completed.stderr)
                self.assertNotIn("Traceback", completed.stderr)


class TestTargetedSuiteWiring(unittest.TestCase):
    def test_targeted_suite_reuses_every_non_python_canonical_phase(self) -> None:
        phases = targeted_phases(("tests.test_pyright_checks",))

        self.assertEqual(
            tuple(phase.name for phase in phases),
            ("js-syntax", "js-unit", "pyright", "ruff", "vulture", "python"),
        )
        python_phase = phases[-1]
        self.assertIs(python_phase.parser, python_tests.parse_failures)
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
