"""Audit: `SHARED_MODULES_WITHOUT_COVERAGE` entries stay uncovered (issue #1095).

`scripts/targeted_test_selection.py::SHARED_MODULES_WITHOUT_COVERAGE` is an
admitted, reviewed claim that a registered shared `tests/` module has "no
real consuming test today" — each entry's rationale says so explicitly
("No test drives EphemeralSlskd directly", "Nothing imports
mirror_harness.py"). That claim was review-only: nothing re-checked it after
the review round that wrote it, so a later PR that added a real import would
silently make the registry's own words false and launder real coverage away
without anything failing.

Issue #1095's decided scope (operator, 2026-08-12 session) is **option 2 —
negative-only bounded audit**: prove a registered module is not imported by
any `tests/` module, using one local syntactic fact with a deliberately
bounded import-statement grammar. Positive/derived coverage verification
(does some OTHER test transitively exercise it through a re-export) is out
of scope by the same decision — `tests/fakes/__init__.py`-style re-export
tracing needs alias/data-flow analysis, which is exactly the semantic-scanner
shape `.claude/rules/code-quality.md` § "Semantic source scanners are
prohibited" forbids.

**Grammar.** Only `ast.Import` / `ast.ImportFrom` nodes are read — module-level
or nested inside a function/class, matching `tests/test_generated_node_worker_
audit.py`'s "read the whole tree with `ast.walk`" shape rather than
`tests/test_hypothesis_profile_audit.py`'s position-sensitive
`tree.body[:first_definition]` slice (this audit does not care WHERE an
import runs, only that a real import statement exists at all — a nested
import still proves the module is a real consumer). A statement counts as an
import of a registered module (dotted as `tests.world_model.mirror_harness`
for `tests/world_model/mirror_harness.py`) in exactly these absolute
(`level == 0`) shapes:

* `import tests.world_model.mirror_harness` (any `alias.name` equal to the
  full dotted path; an `as` alias does not change which module is imported)
* `from tests.world_model import mirror_harness` (`node.module` equals the
  registered module's parent package, one `alias.name` equals its leaf)
* `from tests.world_model.mirror_harness import <anything>` (`node.module`
  equals the full dotted path)

No alias tracking (`import tests.world_model as tw` followed by
`tw.mirror_harness.X` is invisible to this grammar — the same "no alias
tracking" boundary `test_hypothesis_profile_audit.py`'s
`_is_canonical_profile_import` already draws) and no
`__import__`/`importlib.import_module` string resolution. Both are out of
scope by the issue's own decision comment: `mirror_harness`'s registry
rationale explicitly names `scripts/run_world_model_burst.py`'s
`"tests.world_model.mirror_harness"` dynamic-string reference as the
admitted, allowed shape a registered module may still have. Relative imports
(`level > 0`) are excluded too — grepping `tests/` for lines starting with
`from .` finds zero real uses today, so recognising them would only widen
the grammar for a shape this repository does not write, and resolving one
correctly needs the importing module's own package position (closer to
alias tracking than a local syntactic fact).

Scope is every `.py` under `tests/` recursively (the same recursive-walk
shape as `test_hypothesis_profile_audit.py::iter_tests_tree_modules`, not a
`test_*.py` glob) — a registered module's rationale claims nothing under
`tests/` imports it, not merely no discoverable unittest target, so a direct
import from a shared helper module (`tests/fakes/__init__.py`, a `conftest.py`
fixture, …) is exactly the kind of real, non-transitive consumer this audit
must catch too.
"""

from __future__ import annotations

import ast
import tempfile
import unittest
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from scripts.targeted_test_selection import SHARED_MODULES_WITHOUT_COVERAGE

REPO_ROOT = Path(__file__).resolve().parents[1]


class NegativeCoverageAuditError(Exception):
    """A tests-tree module could not be parsed, so it cannot be audited."""


@dataclass(frozen=True)
class NegativeCoverageViolation:
    """A registered "no consumer" module that a real import statement contradicts."""

    registered_path: str
    importer: str
    line: int

    def __str__(self) -> str:
        return (
            f"{self.registered_path} is registered in "
            "SHARED_MODULES_WITHOUT_COVERAGE as having no consumer, but "
            f"{self.importer}:{self.line} imports it"
        )


def _dotted_module(registered_path: str) -> str:
    """``"tests/world_model/mirror_harness.py"`` -> ``"tests.world_model.mirror_harness"``."""
    if not registered_path.endswith(".py"):
        raise NegativeCoverageAuditError(
            f"{registered_path}: registered path must end in .py"
        )
    return registered_path[: -len(".py")].replace("/", ".")


def _import_matches_target(node: ast.stmt, target_module: str) -> bool:
    """True when this statement is an absolute import of ``target_module``."""
    if isinstance(node, ast.Import):
        return any(alias.name == target_module for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        if node.level != 0:
            return False
        module = node.module or ""
        if module == target_module:
            return True
        parts = target_module.split(".")
        parent, leaf = ".".join(parts[:-1]), parts[-1]
        return module == parent and any(alias.name == leaf for alias in node.names)
    return False


def _iter_python_files(tests_dir: Path) -> list[Path]:
    found: list[Path] = []
    for path in sorted(tests_dir.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        found.append(path)
    return found


def find_negative_coverage_violations(
    registry: Mapping[str, str],
    repo_root: Path,
) -> list[NegativeCoverageViolation]:
    """Return one violation per real import of a registered "no consumer" module.

    ``registry`` maps a repo-root-relative path (e.g.
    ``"tests/world_model/mirror_harness.py"``) to its admitted-gap rationale
    — only the keys matter here. Takes ``repo_root`` (not a fixed constant)
    so a self-test can drive it against a synthetic tree instead of the real
    repository.
    """
    tests_dir = repo_root / "tests"
    python_files = _iter_python_files(tests_dir)
    violations: list[NegativeCoverageViolation] = []
    for registered_path in sorted(registry):
        target_module = _dotted_module(registered_path)
        registered_abs = repo_root / registered_path
        for path in python_files:
            if path == registered_abs:
                continue
            relpath = path.relative_to(repo_root).as_posix()
            source = path.read_text(encoding="utf-8")
            try:
                tree = ast.parse(source, filename=relpath)
            except SyntaxError as exc:
                raise NegativeCoverageAuditError(
                    f"{relpath}: could not be parsed, so negative coverage "
                    f"cannot be audited: {exc}"
                ) from exc
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)) and (
                    _import_matches_target(node, target_module)
                ):
                    violations.append(
                        NegativeCoverageViolation(
                            registered_path, relpath, node.lineno
                        )
                    )
    return violations


class TestNegativeCoverageAudit(unittest.TestCase):
    """The real registry, verified against the real tests/ tree."""

    def test_registered_modules_have_no_importers(self) -> None:
        violations = find_negative_coverage_violations(
            SHARED_MODULES_WITHOUT_COVERAGE, REPO_ROOT
        )
        self.assertEqual(
            violations,
            [],
            "A module registered in SHARED_MODULES_WITHOUT_COVERAGE as "
            "having no consumer is actually imported somewhere under "
            "tests/, which makes the registry's rationale false and "
            "launders real coverage away:\n  "
            + "\n  ".join(str(violation) for violation in violations),
        )

    def test_registry_is_non_empty(self) -> None:
        """Pin: this audit has something real to check today."""
        self.assertTrue(SHARED_MODULES_WITHOUT_COVERAGE)


class TestNegativeCoverageCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: a fabricated registry entry that IS imported
    must trip, driven through a synthetic tests/ tree — never the real
    registry — so the checker's own correctness is proven independently of
    today's registrations happening to be clean."""

    def _world(
        self, tmp: str, *, target_relpath: str, importer_relpath: str, importer_source: str
    ) -> Path:
        repo_root = Path(tmp)
        target = repo_root / target_relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text("VALUE = 1\n", encoding="utf-8")
        importer = repo_root / importer_relpath
        importer.parent.mkdir(parents=True, exist_ok=True)
        importer.write_text(importer_source, encoding="utf-8")
        return repo_root

    def test_absolute_module_import_trips(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import tests.leaf_target\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(
                violations,
                [
                    NegativeCoverageViolation(
                        "tests/leaf_target.py", "tests/test_importer.py", 1
                    )
                ],
            )

    def test_aliased_absolute_import_still_trips(self) -> None:
        """An ``as`` alias renames the LOCAL binding, not which module is
        imported — the grammar must not be fooled by it."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import tests.leaf_target as lt\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].registered_path, "tests/leaf_target.py")
            self.assertEqual(violations[0].importer, "tests/test_importer.py")

    def test_from_package_import_leaf_shape_trips(self) -> None:
        """``from tests import ephemeral_slskd`` shape."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests import leaf_target\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].importer, "tests/test_importer.py")

    def test_from_subpackage_import_leaf_shape_trips(self) -> None:
        """``from tests.world_model import mirror_harness`` shape."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/world_model/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests.world_model import leaf_target\n",
            )
            registry = {
                "tests/world_model/leaf_target.py": "synthetic gap for self-test"
            }

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(
                violations[0].registered_path, "tests/world_model/leaf_target.py"
            )

    def test_from_full_dotted_path_import_name_shape_trips(self) -> None:
        """``from tests.leaf_target import SOMETHING`` shape."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests.leaf_target import VALUE\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)

    def test_nested_import_inside_a_function_trips(self) -> None:
        """A deferred import inside a function body is still a real import —
        the grammar walks the whole tree, not just module-level statements."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source=(
                    "def helper():\n"
                    "    import tests.leaf_target\n"
                    "    return tests.leaf_target\n"
                ),
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].line, 2)

    def test_non_test_shared_module_importer_still_trips(self) -> None:
        """A direct import from shared tests/ infrastructure (not a
        ``test_*.py`` file) is exactly the kind of real, non-transitive
        consumer the registry's own rationale claims does not exist."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/fakes/__init__.py",
                importer_source="import tests.leaf_target\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].importer, "tests/fakes/__init__.py")

    def test_unrelated_import_does_not_trip(self) -> None:
        """Must-still-work guard: an importer that imports something else
        entirely must not be flagged."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import os\nimport unittest\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_relative_import_is_out_of_scope(self) -> None:
        """Relative imports (``level > 0``) are deliberately unrecognised —
        the repository writes zero of them under tests/ today, so widening
        the grammar to resolve one would only add unused complexity."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/world_model/leaf_target.py",
                importer_relpath="tests/world_model/test_importer.py",
                importer_source="from . import leaf_target\n",
            )
            registry = {
                "tests/world_model/leaf_target.py": "synthetic gap for self-test"
            }

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_dynamic_string_resolution_is_out_of_scope(self) -> None:
        """``importlib.import_module("tests.leaf_target")`` is the admitted,
        allowed shape a registered module may still have — the issue's own
        decision comment names ``run_world_model_burst.py``'s dynamic-string
        reference to ``mirror_harness`` as exactly this."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source=(
                    "import importlib\n"
                    'importlib.import_module("tests.leaf_target")\n'
                ),
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_a_mention_inside_a_string_or_comment_is_not_an_import(self) -> None:
        """A regex/substring checker would misfire on this; ast.parse does
        not."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source=(
                    '"""See import tests.leaf_target for context."""\n'
                    "# import tests.leaf_target\n"
                    'SNIPPET = "import tests.leaf_target"\n'
                ),
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_registered_module_with_no_importer_produces_no_violation(self) -> None:
        """Must-still-work guard: today's clean registration shape."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_unrelated.py",
                importer_source="import unittest\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_unparseable_module_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_broken.py",
                importer_source="def broken(:\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            with self.assertRaisesRegex(
                NegativeCoverageAuditError, "tests/test_broken.py"
            ):
                find_negative_coverage_violations(registry, repo_root)

    def test_malformed_registered_path_fails_closed(self) -> None:
        self.assertRaisesRegex(
            NegativeCoverageAuditError,
            "must end in .py",
            _dotted_module,
            "tests/leaf_target",
        )


if __name__ == "__main__":
    unittest.main()
