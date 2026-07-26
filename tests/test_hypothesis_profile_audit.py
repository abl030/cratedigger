"""Audit: every Hypothesis-using module under tests/ loads the shared tiers.

``tests/_hypothesis_profiles.py`` registers the ``suite`` and ``fuzz`` tiers
and loads the selected one **as an import side effect**. A ``@given`` or
``@settings(...)`` resolves every knob it does not name from
``settings.default`` at decoration time, so a module that never imports the
profile module gets whatever default happened to be loaded when it was
imported — the registered tier if some earlier module pulled it in, stock
Hypothesis defaults if not. That makes the effective budget, determinism and
deadline of those tests a function of unittest's module load order.

Two real consequences were live on ``main`` before this audit:

* ``tests/test_dispatch_quarantine_root_generated.py`` and
  ``tests/test_force_import_service_generated.py`` carried bare ``@given``
  decorators, so they inherited the stock 200ms deadline. That is a hard
  failure in ``scripts/run_fuzz_tests.py::_discover_module_child``, which
  refuses any Hypothesis test with a non-``None`` effective deadline — the
  default fuzz burst could not run at all.
* ``tests/test_final_gate_receipt_generated.py`` pinned ``deadline=None``
  explicitly, so it was not a burst blocker, but it still ran
  non-derandomized and example-database-backed inside the ``suite`` tier,
  whose whole promise is machine-identical runs.

**Scope is every ``.py`` under ``tests/`` recursively, not the
``test_*_generated.py`` glob.** ``tests/test_album_source.py`` is the reason:
it carries a bare ``@given`` while sitting outside that glob, so the burst
runner never even sees it, yet it drags the stock 200ms deadline into the
ordinary suite. The same argument covers the non-``test_``-prefixed helper
modules (``tests/world_model/``) — they construct ``settings`` objects at
import time and are subject to the identical ordering hazard. The single
structural exclusion is the profile module itself, which cannot import
itself.

The grammar is deliberately small: one canonical, module-level
``import tests._hypothesis_profiles`` statement. Anything else — an alias, a
``from``-import, a statement nested inside a function or an ``if`` — fails
closed and is reported as non-canonical rather than silently accepted. This
audit reads import statements with the stdlib ``ast`` and nothing else: no
data flow, no alias tracking, no inference about runtime behaviour (see
``.claude/rules/code-quality.md`` § "Semantic source scanners are
prohibited").
"""

from __future__ import annotations

import ast
import os
import unittest
from dataclasses import dataclass


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))

#: The one sanctioned spelling. Everything else fails closed.
CANONICAL_PROFILE_MODULE = "tests._hypothesis_profiles"

#: Relative path of the profile module itself — the only structural exclusion.
PROFILE_MODULE_RELPATH = "_hypothesis_profiles.py"

_PROFILE_LEAF = "_hypothesis_profiles"


class HypothesisProfileAuditError(Exception):
    """A tests-tree module could not be classified, so the audit fails."""


@dataclass(frozen=True)
class ModuleProfileFacts:
    """Bounded syntactic facts about one module's import statements."""

    uses_hypothesis: bool
    canonical_profile_import: bool
    other_profile_import: bool


def _imports_hypothesis(node: ast.stmt) -> bool:
    """True when this statement imports the third-party ``hypothesis``."""
    if isinstance(node, ast.Import):
        return any(
            alias.name == "hypothesis" or alias.name.startswith("hypothesis.")
            for alias in node.names
        )
    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        return node.level == 0 and (
            module == "hypothesis" or module.startswith("hypothesis.")
        )
    return False


def _mentions_profile_module(node: ast.stmt) -> bool:
    """True when this import statement names the profile module at all."""
    if isinstance(node, ast.Import):
        return any(
            alias.name.split(".")[-1] == _PROFILE_LEAF for alias in node.names
        )
    if isinstance(node, ast.ImportFrom):
        module = node.module or ""
        if module.split(".")[-1] == _PROFILE_LEAF:
            return True
        return any(alias.name == _PROFILE_LEAF for alias in node.names)
    return False


def _is_canonical_profile_import(node: ast.stmt) -> bool:
    """True for exactly ``import tests._hypothesis_profiles`` (no alias)."""
    return isinstance(node, ast.Import) and any(
        alias.name == CANONICAL_PROFILE_MODULE and alias.asname is None
        for alias in node.names
    )


def module_profile_facts(source: str, *, label: str) -> ModuleProfileFacts:
    """Classify one module source by its import statements alone.

    Hypothesis usage counts wherever it appears; the canonical profile import
    counts only as a direct child of the module body, because a statement
    nested in a function or an ``if`` does not necessarily run at import time.
    A source that does not parse raises — the audit never passes a module it
    could not read.
    """
    try:
        tree = ast.parse(source, filename=label)
    except SyntaxError as exc:
        raise HypothesisProfileAuditError(
            f"{label}: could not be parsed, so its Hypothesis profile "
            f"wiring cannot be audited: {exc}"
        ) from exc

    statements = [node for node in ast.walk(tree) if isinstance(node, ast.stmt)]
    uses_hypothesis = any(_imports_hypothesis(node) for node in statements)
    canonical = any(_is_canonical_profile_import(node) for node in tree.body)
    mentions_profile = any(_mentions_profile_module(node) for node in statements)
    return ModuleProfileFacts(
        uses_hypothesis=uses_hypothesis,
        canonical_profile_import=canonical,
        other_profile_import=mentions_profile and not canonical,
    )


def profile_import_violation(facts: ModuleProfileFacts) -> str | None:
    """Return the violation for these facts, or ``None`` when compliant."""
    if not facts.uses_hypothesis:
        return None
    if facts.canonical_profile_import:
        return None
    if facts.other_profile_import:
        return (
            "non-canonical profile import — use a module-level "
            f"`import {CANONICAL_PROFILE_MODULE}`"
        )
    return (
        "imports hypothesis without a module-level "
        f"`import {CANONICAL_PROFILE_MODULE}`"
    )


def iter_tests_tree_modules(root: str = TESTS_DIR) -> list[tuple[str, str]]:
    """Yield ``(relpath, abspath)`` for every auditable .py under ``root``."""
    found: list[tuple[str, str]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if d != "__pycache__")
        for filename in sorted(filenames):
            if not filename.endswith(".py"):
                continue
            path = os.path.join(dirpath, filename)
            relpath = os.path.relpath(path, root)
            if relpath == PROFILE_MODULE_RELPATH:
                continue
            found.append((relpath, path))
    return found


def audit_tests_tree(root: str = TESTS_DIR) -> list[str]:
    """Return one offender line per non-compliant module, sorted by path."""
    offenders: list[str] = []
    for relpath, path in iter_tests_tree_modules(root):
        with open(path, encoding="utf-8") as handle:
            source = handle.read()
        facts = module_profile_facts(source, label=relpath)
        violation = profile_import_violation(facts)
        if violation is not None:
            offenders.append(f"{relpath}: {violation}")
    return offenders


class TestHypothesisProfileImportAudit(unittest.TestCase):
    """Every Hypothesis-using tests module wires itself into the tiers."""

    def test_every_hypothesis_module_loads_the_shared_profiles(self) -> None:
        offenders = audit_tests_tree()
        self.assertEqual(
            offenders, [],
            "A Hypothesis test whose module never imports "
            f"`{CANONICAL_PROFILE_MODULE}` resolves its unnamed settings from "
            "an import-order-dependent global default: stock Hypothesis "
            "defaults (200ms deadline, randomized, example-database backed) "
            "instead of the suite/fuzz tier. A stock deadline also aborts "
            "`scripts/run_fuzz_tests.py` in discovery. Offenders "
            "(paths relative to tests/):\n  "
            + "\n  ".join(offenders),
        )

    def test_scan_reaches_every_tests_subpackage(self) -> None:
        """Pin the recursive walk: a revert to a glob or ``os.listdir``
        would silently drop the subpackages that motivated this scope."""
        relpaths = {relpath for relpath, _ in iter_tests_tree_modules()}
        self.assertIn("test_album_source.py", relpaths)
        self.assertIn(os.path.join("web", "_harness.py"), relpaths)
        self.assertIn(os.path.join("world_model", "state_machine.py"), relpaths)

    def test_the_profile_module_is_the_only_exclusion(self) -> None:
        relpaths = {relpath for relpath, _ in iter_tests_tree_modules()}
        self.assertNotIn(PROFILE_MODULE_RELPATH, relpaths)
        self.assertIn(os.path.basename(__file__), relpaths)


class TestHypothesisProfileCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: the checker must reject planted module shapes."""

    HYPOTHESIS_FORMS = (
        "import hypothesis\n",
        "import hypothesis.strategies as st\n",
        "from hypothesis import given\n",
        "from hypothesis.strategies import integers\n",
        "from hypothesis.stateful import rule\n",
    )

    def _violation(self, source: str) -> str | None:
        return profile_import_violation(
            module_profile_facts(source, label="planted.py"),
        )

    def test_hypothesis_without_the_profile_import_is_rejected(self) -> None:
        for form in self.HYPOTHESIS_FORMS:
            with self.subTest(form=form.strip()):
                violation = self._violation(f"{form}\nVALUE = 1\n")
                self.assertIsNotNone(violation)
                assert violation is not None
                self.assertIn("without a module-level", violation)

    def test_hypothesis_with_the_canonical_profile_import_passes(self) -> None:
        for form in self.HYPOTHESIS_FORMS:
            with self.subTest(form=form.strip()):
                self.assertIsNone(
                    self._violation(
                        f"{form}import {CANONICAL_PROFILE_MODULE}  # noqa: F401\n",
                    ),
                )

    def test_a_module_without_hypothesis_needs_no_profile_import(self) -> None:
        self.assertIsNone(self._violation("import os\nimport unittest\n"))

    def test_non_canonical_profile_imports_fail_closed(self) -> None:
        non_canonical = (
            f"import {CANONICAL_PROFILE_MODULE} as profiles\n",
            "from tests import _hypothesis_profiles\n",
            f"from {CANONICAL_PROFILE_MODULE} import settings\n",
            f"def wire():\n    import {CANONICAL_PROFILE_MODULE}\n",
            f"if True:\n    import {CANONICAL_PROFILE_MODULE}\n",
        )
        for form in non_canonical:
            with self.subTest(form=form.replace("\n", " ").strip()):
                violation = self._violation(
                    f"from hypothesis import given\n{form}",
                )
                self.assertIsNotNone(violation)
                assert violation is not None
                self.assertIn("non-canonical", violation)

    def test_a_mention_inside_a_string_is_not_an_import(self) -> None:
        """A regex/substring checker would pass this planted source."""
        source = (
            '"""This module explains `import tests._hypothesis_profiles`."""\n'
            "from hypothesis import given\n"
            'SNIPPET = "import tests._hypothesis_profiles  # noqa: F401"\n'
        )
        self.assertIn("import tests._hypothesis_profiles", source)

        violation = self._violation(source)

        self.assertIsNotNone(violation)
        assert violation is not None
        self.assertIn("without a module-level", violation)

    def test_an_unparseable_module_fails_closed(self) -> None:
        with self.assertRaises(HypothesisProfileAuditError):
            module_profile_facts("def broken(:\n", label="planted.py")

    def test_a_deferred_hypothesis_import_is_still_in_scope(self) -> None:
        """Hypothesis usage counts wherever it appears; the profile import
        must still be module-level."""
        facts = module_profile_facts(
            "def build():\n    from hypothesis import strategies as st\n"
            "    return st\n",
            label="planted.py",
        )
        self.assertTrue(facts.uses_hypothesis)
        self.assertIsNotNone(profile_import_violation(facts))


if __name__ == "__main__":
    unittest.main()
