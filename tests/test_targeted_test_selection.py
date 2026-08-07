"""Deterministic contracts for targeted plus adjacent test selection."""

from __future__ import annotations

import unittest
from pathlib import Path

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


if __name__ == "__main__":
    unittest.main()
