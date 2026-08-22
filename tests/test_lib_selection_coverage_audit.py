"""Audit: `LIB_MODULES_WITHOUT_SELECTION_COVERAGE` stays exact (issue #1199).

`scripts/targeted_test_selection.py::LIB_MODULES_WITHOUT_SELECTION_COVERAGE`
is the lib/ twin of `SHARED_MODULES_WITHOUT_COVERAGE` — a changed
`lib/**/*.py` file whose full neighbour resolution (`EXACT_PATH_NEIGHBOURS` +
prefix rules + direct candidates that actually exist) yields zero test
modules now fails closed in `_changed_path_neighbours` (an exit-code-2
refusal through `scripts/test.sh`, the same shape the tests/-side unmapped-
module check already had) unless the path is admitted here.

Unlike `SHARED_MODULES_WITHOUT_COVERAGE`, `_changed_path_neighbours` does
NOT early-return for a registered lib/ path — the full resolution still
runs, so a registration can go stale the moment someone adds a real
`EXACT_PATH_NEIGHBOURS` entry, a new prefix rule, or a `tests.test_<stem>`
module for it, and the registry's own claim ("resolves zero test
neighbours") silently becomes false with nothing to say so. This audit
proves the registry exact by construction in BOTH directions, driving the
REAL resolution function (not a reimplementation):

1. every registered path still resolves zero neighbours (else it is a
   STALE admission and must be removed); and
2. every `lib/**/*.py` file NOT registered here still resolves at least one
   neighbour (else it is an UNLISTED gap and must be registered, or given
   real coverage).

This is deliberately test infrastructure (selection machinery), so — per
`.claude/rules/code-quality.md` § "Never property-test the test machinery"
— it is a deterministic audit, no generated property. Direction 2 is proven
for free by `_changed_path_neighbours` itself: an unregistered zero-
neighbour lib/ path raises `ValueError` naming the path, so simply calling
it for every real `lib/**/*.py` file (inside a `subTest` so unittest reports
every offender, not just the first) is the check.
"""

from __future__ import annotations

import contextlib
import io
import unittest
from collections.abc import Mapping
from pathlib import Path

from scripts.targeted_test_selection import (
    LIB_MODULES_WITHOUT_SELECTION_COVERAGE,
    _changed_path_neighbours,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


def _stale_lib_selection_gaps(
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
                "LIB_MODULES_WITHOUT_SELECTION_COVERAGE as resolving zero "
                f"test neighbours, but now resolves: {', '.join(neighbours)}"
            )
    return violations


class TestLibSelectionCoverageRegistryIsExact(unittest.TestCase):
    """The real registry, verified against the real resolution function."""

    def test_no_registered_gap_is_stale(self) -> None:
        violations = _stale_lib_selection_gaps(
            LIB_MODULES_WITHOUT_SELECTION_COVERAGE, REPO_ROOT
        )
        self.assertEqual(
            violations,
            [],
            "A path registered in LIB_MODULES_WITHOUT_SELECTION_COVERAGE "
            "as a zero-neighbour gap now resolves real coverage — remove "
            "the stale registration:\n  " + "\n  ".join(violations),
        )

    def test_registry_is_non_empty(self) -> None:
        """Pin: this audit has something real to check today."""
        self.assertTrue(LIB_MODULES_WITHOUT_SELECTION_COVERAGE)

    def test_registered_paths_carry_a_non_empty_rationale(self) -> None:
        for path, rationale in LIB_MODULES_WITHOUT_SELECTION_COVERAGE.items():
            with self.subTest(path=path):
                self.assertTrue(
                    rationale.strip(),
                    f"{path} is registered with an empty rationale",
                )

    def test_registered_paths_still_exist_on_disk(self) -> None:
        for path in LIB_MODULES_WITHOUT_SELECTION_COVERAGE:
            with self.subTest(path=path):
                self.assertTrue(
                    (REPO_ROOT / path).is_file(),
                    f"registered lib/ gap does not exist on disk: {path}",
                )

    def test_every_lib_module_resolves_or_is_registered(self) -> None:
        """Tree-walking pin: every real `lib/**/*.py` file either resolves
        at least one neighbour, or is admitted in
        LIB_MODULES_WITHOUT_SELECTION_COVERAGE. Calls the REAL
        `_changed_path_neighbours` for every file — an unregistered
        zero-neighbour file raises `ValueError` naming itself, which
        `subTest` reports as that file's own failure without stopping the
        walk (so every offender is named in one run, not just the first).
        """
        lib_files = sorted(
            path for path in (REPO_ROOT / "lib").rglob("*.py")
            if "__pycache__" not in path.parts
        )
        self.assertTrue(lib_files, "expected lib/**/*.py files to exist")

        for path in lib_files:
            relative = path.relative_to(REPO_ROOT).as_posix()
            with self.subTest(path=relative):
                _changed_path_neighbours(relative, REPO_ROOT)


class TestLibSelectionCoverageCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests proving the checker's own correctness, split
    two ways (issue #1248 review correction — a prior version of this
    docstring claimed ALL six tests below use only synthetic registries/
    paths; three do not). The two "fails closed" tests (unmapped
    top-level / nested lib/ paths) and the stale-registration trip test
    drive entirely synthetic paths/registries never present in the real
    module-level data. The remaining three ("admitted gap log names
    itself", "registered zero-neighbour gap does not raise", "stale
    checker is quiet on a genuine gap") deliberately drive the REAL
    registry's first entry via
    `next(iter(LIB_MODULES_WITHOUT_SELECTION_COVERAGE))` — they are
    must-still-work controls proving a real admitted gap behaves
    correctly, not proof the checker fires on a violation; they depend on
    today's registry actually holding a genuine, non-stale gap, which
    `TestLibSelectionCoverageRegistryIsExact` above independently
    proves."""

    def test_unmapped_zero_neighbour_lib_file_fails_closed_with_its_name(
        self,
    ) -> None:
        """Direction 2 (unlisted gap): a lib/ path with zero neighbours
        that is NOT registered must raise, naming itself. The path need
        not exist on disk — `_changed_path_neighbours` never stats its
        own target, only candidate test modules (mirrors the existing
        tests/-side known-bad self-test's same non-existent-path shape,
        `test_unmapped_shared_test_module_fails_closed_with_the_file_name`
        in tests/test_targeted_test_selection.py)."""
        with self.assertRaisesRegex(
            ValueError,
            r"lib/_totally_unmapped_selection_probe\.py",
        ):
            _changed_path_neighbours(
                "lib/_totally_unmapped_selection_probe.py", REPO_ROOT
            )

    def test_unmapped_nested_lib_file_fails_closed_with_its_name(
        self,
    ) -> None:
        """Direction 2, NESTED shape (issue #1199 review F2): the
        top-level probe above (`lib/_totally_unmapped_selection_probe.py`,
        `len(path.parts) == 2`) cannot by itself distinguish the real
        `path.parts[:1] == ("lib",)` guard from a narrower
        `len(path.parts) == 2` mutant — every nested lib/ file in the real
        tree today happens to be registered, so that survivor was never
        exercised. This probe is nested three levels deep
        (`len(path.parts) == 3`) and unregistered, so only the real
        first-component guard, not a top-level-only narrowing, can make
        this raise."""
        with self.assertRaisesRegex(
            ValueError,
            r"lib/dispatch/_totally_unmapped_nested_probe\.py",
        ):
            _changed_path_neighbours(
                "lib/dispatch/_totally_unmapped_nested_probe.py", REPO_ROOT
            )

    def test_admitted_gap_log_names_the_path_and_its_rationale(
        self,
    ) -> None:
        """The loud admitted-gap stderr line (issue #1199 review F4) must
        name BOTH the registered path AND its registered rationale — not
        merely print SOME line. Captures the real stderr `_changed_path_
        neighbours` writes for a real registered path, driven through the
        public entry point."""
        registered = next(iter(LIB_MODULES_WITHOUT_SELECTION_COVERAGE))
        rationale = LIB_MODULES_WITHOUT_SELECTION_COVERAGE[registered]
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
        registered = next(iter(LIB_MODULES_WITHOUT_SELECTION_COVERAGE))
        self.assertEqual(
            _changed_path_neighbours(registered, REPO_ROOT), ()
        )

    def test_stale_registration_checker_trips_on_a_planted_stale_entry(
        self,
    ) -> None:
        """Direction 1 (stale admission): a fabricated registry entry
        naming a lib/ path the real prefix rules actually cover (here,
        the lib/pipeline_db/ prefix rule, which unconditionally extends
        PIPELINE_DB_NEIGHBOURS regardless of whether the file exists) must
        be reported as stale, naming the path and what it now resolves
        to. The real module-level registry is never touched."""
        fabricated = {
            "lib/pipeline_db/_selection_probe.py": (
                "synthetic stale entry for self-test"
            ),
        }

        violations = _stale_lib_selection_gaps(fabricated, REPO_ROOT)

        self.assertEqual(len(violations), 1)
        self.assertIn("lib/pipeline_db/_selection_probe.py", violations[0])
        self.assertIn("tests.test_pipeline_db", violations[0])

    def test_stale_registration_checker_is_quiet_on_a_genuine_gap(
        self,
    ) -> None:
        """Must-still-work: a fabricated registry entry that genuinely
        resolves zero neighbours (an existing real gap) produces no
        violation."""
        registered = next(iter(LIB_MODULES_WITHOUT_SELECTION_COVERAGE))
        fabricated = {registered: "synthetic rationale for self-test"}

        violations = _stale_lib_selection_gaps(fabricated, REPO_ROOT)

        self.assertEqual(violations, [])


if __name__ == "__main__":
    unittest.main()
