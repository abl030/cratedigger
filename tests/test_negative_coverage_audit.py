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
for `tests/world_model/mirror_harness.py`, with leaf `mirror_harness`) in
exactly these absolute (`level == 0`) shapes:

* `import tests.world_model.mirror_harness` (any `alias.name` equal to the
  full dotted path; an `as` alias does not change which module is imported)
* `from tests.world_model import mirror_harness` (`node.module` equals the
  registered module's parent package, one `alias.name` equals its leaf) —
  also covers `from tests.world_model import mirror_harness as mh`, a
  multi-name `from tests.world_model import os, mirror_harness`, and
  `from tests.world_model import *` does NOT trip this clause (no `alias.name`
  equals the leaf), which is correct: a star-import cannot be proven to reach
  a specific submodule without executing it
* `from tests.world_model.mirror_harness import <anything>` (`node.module`
  equals the full dotted path) — including `from ...mirror_harness import *`
* **The bare-leaf convention**: `sys.path.append(os.path.dirname(__file__))`
  followed by `import mirror_harness` or `from mirror_harness import X`, with
  no `tests.` prefix at all. This is not a hypothetical shape — it is the
  repository's own sanctioned convention: `tests/test_mock_audit.py`'s
  `TestSysPathAudit` is the policy that allows appending a `tests/` (sub)dir
  to `sys.path`, 24 existing test files already do it (e.g.
  `tests/test_deploy_hold.py`'s `sys.path.append(os.path.dirname(__file__))`
  + bare `import conftest`), and `tests/ephemeral_slskd.py`'s own docstring
  documents `from ephemeral_slskd import EphemeralSlskd` as ITS usage. A
  grammar that only matched fully-dotted forms would report a registered
  module "uncovered" while missing exactly the import shape its own docstring
  advertises. Recognised via an exact (never `startswith`) match on the last
  dotted component: `alias.name.split(".")[-1] == leaf` for `ast.Import`, and
  `module.split(".")[-1] == leaf` for `ast.ImportFrom` — the same one-piece
  boundary `tests/test_hypothesis_profile_audit.py::_mentions_profile_module`
  already draws for its own profile-module leaf, including that function's
  accepted trade-off: an unrelated module that happens to share the same leaf
  name would also match. Neither registered leaf (`ephemeral_slskd`,
  `mirror_harness`) collides with any other import under `tests/` today
  (verified by grep), and exact-equality (not `startswith`) means a
  similarly-named module (`ephemeral_slskd_extra`) does not falsely match —
  pinned below as a must-still-work guard, both for the dotted prefix shape
  (`import tests.ephemeral_slskd_extra`) and the bare shape
  (`import ephemeral_slskd_extra`).

No alias tracking (`import tests.world_model as tw` followed by
`tw.mirror_harness.X` is invisible to this grammar) and no
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
must catch too. Each file is read and `ast.parse`d exactly once and checked
against every registry entry from that single parse — not once per entry —
since this audit runs on every `scripts/test.sh` invocation as an ambient
gate.

**Fail-closed edges.** A registry key not ending in `.py`, a `tests/`
directory that is missing or contains zero `.py` files, an unparseable
importer module, and an importer module that is not valid UTF-8 all raise
`NegativeCoverageAuditError` naming the offending path — the audit never
silently treats "could not read this file" as "this file does not import the
target". The one boundary deliberately NOT hardened: an unparseable
*registered* module itself. This audit patrols importers of a registered
module, not the registered module's own health, and its own parseability
cannot hide a real importer elsewhere in the tree — the general per-file
parse guard above already covers it uniformly with every other file, so no
special-case handling was added.
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
    """A registry entry or a tests-tree module could not be audited."""


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
    """True for an absolute import of ``target_module`` — dotted, or the
    repository's sanctioned bare-leaf convention (see module docstring).
    Leaf matching is exact-string only, never ``startswith``.
    """
    parts = target_module.split(".")
    leaf = parts[-1]
    parent = ".".join(parts[:-1])
    if isinstance(node, ast.Import):
        return any(
            alias.name == target_module or alias.name.split(".")[-1] == leaf
            for alias in node.names
        )
    if isinstance(node, ast.ImportFrom):
        if node.level != 0:
            return False
        module = node.module or ""
        if module == target_module or module.split(".")[-1] == leaf:
            return True
        return module == parent and any(alias.name == leaf for alias in node.names)
    return False


def _iter_python_files(tests_dir: Path) -> list[Path]:
    if not tests_dir.is_dir():
        return []
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
    repository. Each file under ``repo_root/tests`` is read and parsed
    exactly once, then checked against every registry entry — not once per
    entry per file.
    """
    tests_dir = repo_root / "tests"
    python_files = _iter_python_files(tests_dir)
    if not python_files:
        raise NegativeCoverageAuditError(
            f"{tests_dir}: no .py files found under tests/ — the audit "
            "cannot prove a negative over a missing or empty tree"
        )
    targets = tuple(
        (registered_path, _dotted_module(registered_path), repo_root / registered_path)
        for registered_path in sorted(registry)
    )
    violations: list[NegativeCoverageViolation] = []
    for path in python_files:
        relpath = path.relative_to(repo_root).as_posix()
        try:
            source = path.read_text(encoding="utf-8")
        except UnicodeDecodeError as exc:
            raise NegativeCoverageAuditError(
                f"{relpath}: could not be read as UTF-8, so negative "
                f"coverage cannot be audited: {exc}"
            ) from exc
        try:
            tree = ast.parse(source, filename=relpath)
        except SyntaxError as exc:
            raise NegativeCoverageAuditError(
                f"{relpath}: could not be parsed, so negative coverage "
                f"cannot be audited: {exc}"
            ) from exc
        import_nodes = [
            node for node in ast.walk(tree)
            if isinstance(node, (ast.Import, ast.ImportFrom))
        ]
        if not import_nodes:
            continue
        for registered_path, target_module, registered_abs in targets:
            if path == registered_abs:
                continue
            for node in import_nodes:
                if _import_matches_target(node, target_module):
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

    # -- dotted shapes -----------------------------------------------------

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

    def test_from_tests_import_leaf_as_alias_trips(self) -> None:
        """``from tests import leaf_target as lt`` shape (SHOULD-FIX #2)."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests import leaf_target as lt\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)

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

    def test_from_full_dotted_path_star_import_trips(self) -> None:
        """``from tests.leaf_target import *`` shape (SHOULD-FIX #2)."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests.leaf_target import *\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)

    def test_from_parent_package_star_import_does_not_trip(self) -> None:
        """Must-still-work, backing the docstring's explicit claim: a
        package-level ``from tests.world_model import *`` does NOT name the
        leaf ``mirror_harness`` at all (no ``alias.name`` equals it, module
        is the PARENT not the full path) — unlike the full-dotted-path star
        import above, which names the target module directly."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/world_model/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from tests.world_model import *\n",
            )
            registry = {
                "tests/world_model/leaf_target.py": "synthetic gap for self-test"
            }

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_multi_alias_import_trips(self) -> None:
        """``import os, tests.leaf_target`` shape (SHOULD-FIX #2) — one
        violation per matching import STATEMENT, not per alias."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import os, tests.leaf_target\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].line, 1)

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

    # -- bare-leaf shapes (MUST-FIX) ----------------------------------------

    def test_bare_import_of_leaf_trips(self) -> None:
        """``sys.path.append(os.path.dirname(__file__)); import leaf_target``
        — the convention 24 existing files use (e.g. tests/test_deploy_hold.py
        + bare ``import conftest``) and tests/ephemeral_slskd.py's own
        docstring documents as ITS usage. This was the MUST-FIX gap: the
        prior dotted-only grammar reported 0 violations for this real shape."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source=(
                    "import sys, os\n"
                    "sys.path.append(os.path.dirname(__file__))\n"
                    "import leaf_target\n"
                ),
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)
            self.assertEqual(violations[0].importer, "tests/test_importer.py")

    def test_bare_from_import_trips(self) -> None:
        """``from leaf_target import X`` bare shape (MUST-FIX)."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="from leaf_target import VALUE\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)

    def test_bare_import_of_nested_leaf_trips(self) -> None:
        """The bare shape also applies to a nested target (the real
        mirror_harness.py's own nesting depth), not just a top-level one."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/world_model/leaf_target.py",
                importer_relpath="tests/world_model/test_importer.py",
                importer_source="import leaf_target\n",
            )
            registry = {
                "tests/world_model/leaf_target.py": "synthetic gap for self-test"
            }

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(len(violations), 1)

    # -- must-still-work / no false positives -------------------------------

    def test_bare_leaf_similar_name_does_not_match(self) -> None:
        """Must-still-work: exact equality only, never ``startswith`` — a
        bare import of a similarly-named module must NOT match."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import leaf_target_extra\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

    def test_prefix_collision_absolute_import_does_not_match(self) -> None:
        """Must-still-work (SHOULD-FIX #1): guards a future ``==`` ->
        ``startswith`` regression on the dotted-form check too."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_importer.py",
                importer_source="import tests.leaf_target_extra\n",
            )
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            violations = find_negative_coverage_violations(registry, repo_root)

            self.assertEqual(violations, [])

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

    # -- fail-closed edges ---------------------------------------------------

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

    def test_non_utf8_module_fails_closed(self) -> None:
        """SHOULD-FIX #4c: a non-UTF-8 importer file is wrapped into
        NegativeCoverageAuditError, not left as a bare UnicodeDecodeError."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = self._world(
                tmp,
                target_relpath="tests/leaf_target.py",
                importer_relpath="tests/test_unrelated.py",
                importer_source="import unittest\n",
            )
            bad = repo_root / "tests" / "test_bad_encoding.py"
            bad.write_bytes(b"import os\n# \xff\xfe is not valid utf-8\n")
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            with self.assertRaisesRegex(
                NegativeCoverageAuditError, "tests/test_bad_encoding.py"
            ):
                find_negative_coverage_violations(registry, repo_root)

    def test_malformed_registered_path_fails_closed(self) -> None:
        """SHOULD-FIX #4a: driven through the PUBLIC entry point, not the
        private ``_dotted_module``, with the ``.`` escaped in the pattern."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            unrelated = repo_root / "tests" / "test_unrelated.py"
            unrelated.parent.mkdir(parents=True, exist_ok=True)
            unrelated.write_text("import unittest\n", encoding="utf-8")
            registry = {"tests/leaf_target": "malformed synthetic entry for self-test"}

            with self.assertRaisesRegex(
                NegativeCoverageAuditError, r"must end in \.py"
            ):
                find_negative_coverage_violations(registry, repo_root)

    def test_empty_tests_directory_fails_closed(self) -> None:
        """SHOULD-FIX #4b: a tests/ dir that exists but holds zero .py files."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            (repo_root / "tests").mkdir(parents=True)
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            with self.assertRaisesRegex(NegativeCoverageAuditError, r"no \.py files"):
                find_negative_coverage_violations(registry, repo_root)

    def test_missing_tests_directory_fails_closed(self) -> None:
        """SHOULD-FIX #4b, other half: no tests/ directory at all."""
        with tempfile.TemporaryDirectory() as tmp:
            repo_root = Path(tmp)
            registry = {"tests/leaf_target.py": "synthetic gap for self-test"}

            with self.assertRaisesRegex(NegativeCoverageAuditError, r"no \.py files"):
                find_negative_coverage_violations(registry, repo_root)


if __name__ == "__main__":
    unittest.main()
