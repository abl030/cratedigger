"""Deterministic contracts for targeted plus adjacent test selection."""

from __future__ import annotations

import subprocess
import unittest
from pathlib import Path

from scripts.run_python_tests import discover_test_modules, select_test_targets
from scripts.run_targeted_tests import targeted_phases
from scripts.targeted_test_selection import (
    ALWAYS_AMBIENT_TESTS,
    ambient_test_modules,
    assert_selection_complete,
    expand_test_selection,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


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
        self-maintaining contract issue #1081 requires. Scoped to tests/
        itself (never REPO_ROOT) so a REPO_ROOT that is itself a
        `.claude/worktrees/` checkout is never crawled: the filter below
        checks the path RELATIVE to REPO_ROOT (never the absolute path,
        which always contains `.claude/worktrees/...` from a worktree
        session) — see #520/#543 for the prior REPO_ROOT-walk incidents.
        """
        modules = discover_test_modules(REPO_ROOT / "tests", REPO_ROOT, "test*.py")
        tests_root = REPO_ROOT / "tests"
        shared_paths = sorted(
            path
            for path in tests_root.rglob("*.py")
            if ".claude" not in path.relative_to(tests_root).parts
            and "__pycache__" not in path.relative_to(tests_root).parts
            and not path.name.startswith("test")
        )
        self.assertTrue(shared_paths, "expected shared tests/ modules to exist")

        for path in shared_paths:
            relative = path.relative_to(REPO_ROOT).as_posix()
            with self.subTest(path=relative):
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
            capture_output=True,
            text=True,
            timeout=180,
            check=False,
        )

        self.assertEqual(
            completed.returncode,
            0,
            completed.stdout + completed.stderr,
        )


if __name__ == "__main__":
    unittest.main()
