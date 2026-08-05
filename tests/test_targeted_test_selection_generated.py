"""Generated completeness contract for targeted test expansion."""

from __future__ import annotations

import unittest
from pathlib import Path

from hypothesis import given
from hypothesis import strategies as st

import tests._hypothesis_profiles  # noqa: F401  (loads the active profile)
from scripts.targeted_test_selection import (
    ambient_test_modules,
    assert_selection_complete,
    expand_test_selection,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
PAIRS = (
    ("tests.test_pyright_checks", "tests.test_pyright_checks_generated"),
    ("tests.test_suite_coordinator", "tests.test_suite_coordinator_generated"),
    (
        "tests.test_targeted_test_selection",
        "tests.test_targeted_test_selection_generated",
    ),
)


class TestGeneratedTargetedSelection(unittest.TestCase):
    @given(
        selected=st.lists(
            st.sampled_from(tuple(module for pair in PAIRS for module in pair)),
            max_size=12,
        )
    )
    def test_expansion_is_unique_and_closes_every_selected_pair(
        self,
        selected: list[str],
    ) -> None:
        expanded = expand_test_selection(
            selected,
            changed_paths=(),
            repo_root=REPO_ROOT,
        )
        required: set[str] = set(ambient_test_modules(REPO_ROOT))
        for deterministic, generated in PAIRS:
            if deterministic in selected or generated in selected:
                required.update((deterministic, generated))

        assert_selection_complete(expanded, tuple(required))


class TestSelectionCheckerKnownBad(unittest.TestCase):
    def test_checker_rejects_the_old_explicit_only_shape(self) -> None:
        with self.assertRaisesRegex(AssertionError, "missing"):
            assert_selection_complete(
                ("tests.test_pyright_checks",),
                (
                    "tests.test_pyright_checks",
                    "tests.test_pyright_checks_generated",
                    *ambient_test_modules(REPO_ROOT),
                ),
            )


if __name__ == "__main__":
    unittest.main()
