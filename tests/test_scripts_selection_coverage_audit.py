"""Audit: `SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE` stays exact (issue #1248).

`scripts/targeted_test_selection.py::SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE`
is the scripts/ twin of `LIB_MODULES_WITHOUT_SELECTION_COVERAGE` — a changed
`scripts/**/*.py` file whose full neighbour resolution
(`EXACT_PATH_NEIGHBOURS` + prefix rules + direct candidates that actually
exist) yields zero test modules now fails closed in
`_changed_path_neighbours` (an exit-code-2 refusal through
`scripts/test.sh`, the same shape the lib/-side unmapped-module check
already had) unless the path is admitted here.

Same non-early-return shape as the lib/ registry: `_changed_path_neighbours`
does NOT return early for a path registered here — the full resolution still
runs, so a registration can go stale the moment someone adds a real
`EXACT_PATH_NEIGHBOURS` entry, a new prefix rule, or a `tests.test_<stem>`
module for it. This audit proves the registry exact by construction in BOTH
directions, driving the REAL resolution function (not a reimplementation):

1. every registered path still resolves zero neighbours (else it is a
   STALE admission and must be removed); and
2. every real `scripts/**/*.py` file NOT registered here still resolves at
   least one neighbour (else it is an UNLISTED gap and must be registered,
   or given real coverage).

This is deliberately test infrastructure (selection machinery), so — per
`.claude/rules/code-quality.md` § "Never property-test the test machinery"
— it is a deterministic audit, no generated property. Direction 2 is proven
for free by `_changed_path_neighbours` itself: an unregistered zero-
neighbour scripts/ path raises `ValueError` naming the path, so simply
calling it for every real `scripts/**/*.py` file (inside a `subTest` so
unittest reports every offender, not just the first) is the check.
"""

from __future__ import annotations

import contextlib
import io
import unittest
from collections.abc import Mapping
from pathlib import Path

from scripts.targeted_test_selection import (
    SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE,
    _changed_path_neighbours,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _stale_scripts_selection_gaps(
    registry: Mapping[str, str],
    repo_root: Path,
) -> list[str]:
    """Return one message per STALE registry entry — a path registered as
    "resolves zero neighbours" that now resolves real ones. Drives the
    REAL `_changed_path_neighbours`, never a reimplementation of its logic.
    """
    violations: list[str] = []
    for path in sorted(registry):
        neighbours = _changed_path_neighbours(path, repo_root)
        if neighbours:
            violations.append(
                f"{path} is registered in "
                "SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE as resolving "
                f"zero test neighbours, but now resolves: "
                f"{', '.join(neighbours)}"
            )
    return violations


class TestScriptsSelectionCoverageRegistryIsExact(unittest.TestCase):
    """The real registry, verified against the real resolution function."""

    def test_no_registered_gap_is_stale(self) -> None:
        violations = _stale_scripts_selection_gaps(
            SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE, REPO_ROOT
        )
        self.assertEqual(
            violations,
            [],
            "A path registered in SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE "
            "as a zero-neighbour gap now resolves real "
            "coverage — remove the stale registration:\n  "
            + "\n  ".join(violations),
        )

    def test_registry_is_non_empty(self) -> None:
        """Pin: this audit has something real to check today."""
        self.assertTrue(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE)

    def test_registered_paths_carry_a_non_empty_rationale(self) -> None:
        for path, rationale in SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE.items():
            with self.subTest(path=path):
                self.assertTrue(
                    rationale.strip(),
                    f"{path} is registered with an empty rationale",
                )

    def test_registered_paths_still_exist_on_disk(self) -> None:
        for path in SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE:
            with self.subTest(path=path):
                self.assertTrue(
                    (REPO_ROOT / path).is_file(),
                    f"registered scripts/ gap does not exist on disk: {path}",
                )

    def test_every_scripts_module_resolves_or_is_registered(self) -> None:
        """Tree-walking pin: every real `scripts/**/*.py` file either
        resolves at least one neighbour, or is admitted in
        SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE. Calls the REAL
        `_changed_path_neighbours` for every file — an unregistered
        zero-neighbour file raises `ValueError` naming itself, which
        `subTest` reports as that file's own failure without stopping the
        walk (so every offender is named in one run, not just the first).
        """
        scripts_files = sorted(
            path for path in (REPO_ROOT / "scripts").rglob("*.py")
            if "__pycache__" not in path.parts
        )
        self.assertTrue(scripts_files, "expected scripts/**/*.py files to exist")

        for path in scripts_files:
            relative = path.relative_to(REPO_ROOT).as_posix()
            with self.subTest(path=relative):
                _changed_path_neighbours(relative, REPO_ROOT)


class TestScriptsSelectionCoverageCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests proving the checker's own correctness, split
    two ways. The two "fails closed" tests (unmapped top-level / nested
    scripts/ paths) and the stale-registration trip test drive entirely
    synthetic paths/registries never present in the real module-level
    data. The remaining three ("admitted gap log names itself",
    "registered zero-neighbour gap does not raise", "stale checker is
    quiet on a genuine gap") deliberately drive the REAL registry's first
    entry via `next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))` —
    they are must-still-work controls proving a real admitted gap behaves
    correctly, not proof the checker fires on a violation; they depend on
    today's registry actually holding a genuine, non-stale gap, which
    `TestScriptsSelectionCoverageRegistryIsExact` above independently
    proves."""

    def test_unmapped_zero_neighbour_scripts_file_fails_closed_with_its_name(
        self,
    ) -> None:
        """Direction 2 (unlisted gap): a scripts/ path with zero
        neighbours that is NOT registered must raise, naming itself. The
        path need not exist on disk — `_changed_path_neighbours` never
        stats its own target, only candidate test modules (mirrors the
        lib/-side known-bad self-test's same non-existent-path shape,
        `test_unmapped_zero_neighbour_lib_file_fails_closed_with_its_name`
        in tests/test_lib_selection_coverage_audit.py)."""
        with self.assertRaisesRegex(
            ValueError,
            r"scripts/_totally_unmapped_selection_probe\.py",
        ):
            _changed_path_neighbours(
                "scripts/_totally_unmapped_selection_probe.py", REPO_ROOT
            )

    def test_unmapped_nested_scripts_file_fails_closed_with_its_name(
        self,
    ) -> None:
        """Direction 2, NESTED shape: the top-level probe above
        (`scripts/_totally_unmapped_selection_probe.py`,
        `len(path.parts) == 2`) cannot by itself distinguish the real
        `path.parts[:1] == ("scripts",)` guard from a narrower
        `len(path.parts) == 2` mutant — every nested scripts/ file in the
        real tree today (scripts/pipeline_cli/*.py) happens to be either
        registered or resolved. This probe is nested inside
        scripts/pipeline_cli/ and unregistered, so only the real
        first-component guard, not a top-level-only narrowing, can make
        this raise."""
        with self.assertRaisesRegex(
            ValueError,
            r"scripts/pipeline_cli/_totally_unmapped_nested_probe\.py",
        ):
            _changed_path_neighbours(
                "scripts/pipeline_cli/_totally_unmapped_nested_probe.py",
                REPO_ROOT,
            )

    def test_admitted_gap_log_names_the_path_and_its_rationale(
        self,
    ) -> None:
        """The loud admitted-gap stderr line must name BOTH the registered
        path AND its registered rationale — not merely print SOME line.
        Captures the real stderr `_changed_path_neighbours` writes for a
        real registered path, driven through the public entry point."""
        registered = next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))
        rationale = SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE[registered]
        buffer = io.StringIO()

        with contextlib.redirect_stderr(buffer):
            result = _changed_path_neighbours(registered, REPO_ROOT)

        self.assertEqual(result, ())
        emitted = buffer.getvalue()
        self.assertIn(registered, emitted)
        self.assertIn(rationale, emitted)

    def test_registered_gap_with_zero_neighbours_does_not_raise(self) -> None:
        """Must-still-work: a genuinely zero-neighbour path that IS
        registered proceeds (ambient-only selection), never raises."""
        registered = next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))
        self.assertEqual(
            _changed_path_neighbours(registered, REPO_ROOT), ()
        )

    def test_stale_registration_checker_trips_on_a_planted_stale_entry(
        self,
    ) -> None:
        """Direction 1 (stale admission): a fabricated registry entry
        naming a scripts/ path with a real EXACT_PATH_NEIGHBOURS mapping
        (here, scripts/targeted_test_selection.py itself) must be reported
        as stale, naming the path and what it now resolves to. The real
        module-level registry is never touched."""
        fabricated = {
            "scripts/targeted_test_selection.py": (
                "synthetic stale entry for self-test"
            ),
        }

        violations = _stale_scripts_selection_gaps(fabricated, REPO_ROOT)

        self.assertEqual(len(violations), 1)
        self.assertIn("scripts/targeted_test_selection.py", violations[0])
        self.assertIn("tests.test_targeted_test_selection", violations[0])

    def test_stale_registration_checker_is_quiet_on_a_genuine_gap(
        self,
    ) -> None:
        """Must-still-work: a fabricated registry entry that genuinely
        resolves zero neighbours (an existing real gap) produces no
        violation."""
        registered = next(iter(SCRIPTS_MODULES_WITHOUT_SELECTION_COVERAGE))
        fabricated = {registered: "synthetic rationale for self-test"}

        violations = _stale_scripts_selection_gaps(fabricated, REPO_ROOT)

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
