"""Deterministic contracts for targeted plus adjacent test selection."""

from __future__ import annotations

import os
import subprocess
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
    _changed_path_neighbours,
    ambient_test_modules,
    assert_selection_complete,
    expand_test_selection,
)

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

        ``tests/fakes/pipeline_db.py`` is not a discoverable test module — it
        must never yield itself (``tests.fakes.pipeline_db``) as a selector.
        The existing ``tests/fakes/`` prefix rule still rescues the change
        with the real consumer, ``tests.test_fakes``.
        """
        selected = expand_test_selection(
            (),
            changed_paths=("tests/fakes/pipeline_db.py",),
            repo_root=REPO_ROOT,
        )

        self.assertNotIn("tests.fakes.pipeline_db", selected)
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
                    # Admitted gap, not silent under-selection: registered by
                    # name with a rationale (MUST FIX 7, #1081 review round).
                    # Assert the absence explicitly rather than accepting it
                    # by omission — a future real mapping added here without
                    # also removing the registry entry would go unnoticed.
                    self.assertEqual(
                        _changed_path_neighbours(relative, REPO_ROOT),
                        (),
                        f"{relative} is registered as uncovered but now "
                        "has a real neighbour — remove it from "
                        "SHARED_MODULES_WITHOUT_COVERAGE",
                    )
                    continue
                selection = expand_test_selection(
                    (),
                    changed_paths=(relative,),
                    repo_root=REPO_ROOT,
                )
                # hotspot_policies={}: this pin only proves every selector
                # resolves to a real discovered module (issue #1081's actual
                # concern) — hotspot method/class sharding is a separate,
                # already-covered concern (tests.test_parallel_test_runner)
                # that needs an expensive discovery-manifest subprocess per
                # hotspot module, irrelevant here.
                select_test_targets(modules, selection, hotspot_policies={})

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
        source = (REPO_ROOT / "scripts" / "test.sh").read_text(encoding="utf-8")

        self.assertIn("scripts/run_targeted_tests.py", source)
        self.assertIn('"$@"', source)
        self.assertNotIn("python3 -m unittest discover", source)

    def test_python_phase_completes_for_a_diff_touching_shared_fakes(self) -> None:
        """End-to-end pin for issue #1081: a real diff touching tests/fakes/*.py
        must not die on selector resolution — PR #1075's exact failure mode,
        where js/pyright/ruff/vulture all pass and the python phase exits 1
        before any test runs.

        CRATEDIGGER_TEST_JOBS=1 pins the nested runner to one worker.
        worker_environment() unconditionally pops TEST_DB_DSN so every
        persistent worker bootstraps its own ephemeral PostgreSQL — at the
        default worker count (half the host's CPUs, capped at 12) that is up
        to 12 nested clusters spun up inside one already-parallel outer suite
        target: the same class of scheduler-contention flake fixed by
        widening the sp.run timeout in
        tests/test_beets_destructive_configs_generated.py elsewhere in this
        PR. Forcing one worker keeps this a real end-to-end subprocess run
        of the actual entrypoint without adding a second flake of the same
        kind.

        A failure here may duplicate an unrelated ambient audit's own
        failure reported elsewhere in the outer suite — expand_test_selection
        always appends every ambient audit, so this real subprocess reruns
        all of them too. That is the accepted cost of driving the real
        entrypoint rather than a hand-picked subset; the tail-truncated
        detail below keeps the duplicate report short.
        """
        selectors = expand_test_selection(
            (),
            changed_paths=("tests/fakes/pipeline_db.py",),
            repo_root=REPO_ROOT,
        )
        python_phase = targeted_phases(selectors)[-1]
        self.assertEqual(python_phase.name, "python")

        completed = subprocess.run(
            python_phase.command,
            cwd=REPO_ROOT,
            env={**os.environ, "CRATEDIGGER_TEST_JOBS": "1"},
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )

        detail = (completed.stdout + completed.stderr)[-4000:]
        self.assertEqual(completed.returncode, 0, detail)


if __name__ == "__main__":
    unittest.main()
