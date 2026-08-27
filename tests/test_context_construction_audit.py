"""Audit: every production ``CratediggerContext(...)`` site is registered
(issue #1278 item 3).

PR #1280 introduced, and then caught, a regression of exactly one shape: a
SECOND ``CratediggerContext`` construction — the inline one in
``_run_phase1`` — silently omitted ``download_ownership``, which made every
download-timeout cleanup fail closed under the ownership gate that same PR
had just introduced. It was found by the PR's own independent review
(commit ``14b8f533``), NOT by any test: the whole suite was green with the
defect present, because nothing constrained the SET of construction sites.
A new site could appear, or an existing one lose a collaborator, without a
single test noticing.

``PRODUCTION_CONTEXT_CONSTRUCTIONS`` below is that missing constraint. It
is DATA a human maintains, one entry per site, each declaring whether the
site wires ``download_ownership`` and why. The audit's only job is to hold
the registry and the tree in exact agreement, in both directions:

1. every registered site still exists (else it is a STALE entry); and
2. every discovered site is registered (else it is an UNREGISTERED site —
   the #1280 shape, failing closed at test time instead of in production);
   and each site's ``download_ownership=`` kwarg matches its declaration.

**The grammar is deliberately bounded** (`.claude/rules/code-quality.md`
§ "Semantic source scanners are prohibited"). This parses with ``ast`` and
recognises exactly two spellings of a construction — ``CratediggerContext(
...)`` and ``<something>.CratediggerContext(...)`` — plus the enclosing
``def``/``class`` chain the site sits in. It infers nothing about
reachability, values, or runtime behaviour: whether the collaborator a site
passes is the RIGHT one is a question for that site's own test
(``tests/test_cycle_summary.py::TestPhase1ContextForwarding``,
``tests/test_convergence_runner_generated.py::TestPhase1ContextCallSite``),
not for this audit.

The one alias route out of that grammar is closed syntactically rather than
semantically: binding the class to another name — ``from lib.context import
CratediggerContext as X``, or ``X = CratediggerContext`` — is itself a
violation, so a construction can never be spelled under a name this audit
cannot see. Genuinely dynamic construction (``globals()[...]``,
``functools.partial``) remains out of grammar and undetected; recognising it
would require inferring runtime semantics from arbitrary source, which is
the prohibited shape. Nothing in this repository does it, and review owns
that boundary.

Deterministic only: this is test infrastructure auditing production shape,
so per `.claude/rules/code-quality.md` § "Never property-test the test
machinery" there is no generated property here.
"""

from __future__ import annotations

import ast
import unittest
from collections.abc import Mapping
from dataclasses import dataclass
from typing import ClassVar

from tests._typing_ratchet_scanner import iter_production_paths

_CLASS_NAME = "CratediggerContext"


@dataclass(frozen=True)
class ContextSite:
    """One registered production construction of ``CratediggerContext``.

    ``wires_download_ownership`` is the declaration the audit enforces
    against the source; ``why`` is the human's reason, enforced by nobody
    and read by the next author deciding whether a new site needs it.
    """

    wires_download_ownership: bool
    why: str


#: Every production ``CratediggerContext(...)`` construction, keyed
#: ``"<repo-relative path>::<enclosing def/class chain>"`` (``<module>``
#: for a top-level one). Hand-maintained: adding a construction means
#: adding an entry here and stating, in ``why``, whether the new context
#: can reach a destructive slskd call.
PRODUCTION_CONTEXT_CONSTRUCTIONS: Mapping[str, ContextSite] = {
    "cratedigger.py::main": ContextSite(
        wires_download_ownership=True,
        why=(
            "The cycle owner. Constructs the one DownloadOwnershipWriter "
            "every other context borrows; without it nothing in the cycle "
            "can prove ledger ownership and every destructive slskd path "
            "fails closed."
        ),
    ),
    "cratedigger.py::build_phase1_context": ContextSite(
        wires_download_ownership=True,
        why=(
            "Phase 1 reaches lib.download._timeout_album -> "
            "cancel_and_delete, which is ownership-gated. The inline "
            "construction this helper replaced omitted the collaborator, "
            "failing every timeout cleanup closed (#1278) -- the defect "
            "this registry exists to make impossible to repeat."
        ),
    ),
    "lib/enqueue.py::prepare_find_download_context": ContextSite(
        wires_download_ownership=True,
        why=(
            "Every find-download worker context. Supplies five of the six "
            "cancel_and_delete call sites, and forwards the owner's writer "
            "by reference (it opens a fresh DB handle per operation "
            "precisely so worker threads can share one instance)."
        ),
    ),
    "scripts/importer.py::_build_runtime_context": ContextSite(
        wires_download_ownership=False,
        why=(
            "The serial importer's runtime context. Has no path to a "
            "destructive slskd call -- it carries slskd=None -- so the "
            "collaborator would be unreachable. Adding a slskd client here "
            "means adding the writer in the same change."
        ),
    ),
    "scripts/import_preview_worker.py::_materialize_automation_authority": (
        ContextSite(
            wires_download_ownership=False,
            why=(
                "The preview worker's materialization context, also "
                "slskd=None. Same reasoning, same condition on any future "
                "change that gives it a slskd client."
            ),
        )
    ),
}


@dataclass(frozen=True)
class DiscoveredSite:
    """One construction found in the tree, with its own source location."""

    key: str
    lineno: int
    wires_download_ownership: bool


def _sites_in_module(rel_path: str, source: str) -> list[DiscoveredSite]:
    """Every ``CratediggerContext(...)`` call in one module's source.

    Walks the tree keeping the enclosing ``def``/``class`` names as the
    site's qualname, so the key survives the line-number churn any edit
    to the file produces.
    """
    found: list[DiscoveredSite] = []

    def visit(node: ast.AST, scope: tuple[str, ...]) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(
                child, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef,
            ):
                visit(child, (*scope, child.name))
                continue
            if isinstance(child, ast.Call) and _is_context_call(child.func):
                found.append(DiscoveredSite(
                    key=f"{rel_path}::{'.'.join(scope) or '<module>'}",
                    lineno=child.lineno,
                    wires_download_ownership=any(
                        kw.arg == "download_ownership"
                        for kw in child.keywords
                    ),
                ))
            visit(child, scope)

    visit(ast.parse(source), ())
    return found


def _is_context_call(func: ast.expr) -> bool:
    """The two recognised spellings, and no others (bounded grammar)."""
    if isinstance(func, ast.Name):
        return func.id == _CLASS_NAME
    if isinstance(func, ast.Attribute):
        return func.attr == _CLASS_NAME
    return False


def _alias_bindings(rel_path: str, source: str) -> list[str]:
    """Names the class is bound to OTHER than its own.

    An alias would let a construction be spelled under a name
    ``_is_context_call`` cannot recognise, so aliasing is itself the
    violation -- a syntactic fact, not an inference about what the alias
    is later used for.
    """
    violations: list[str] = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            violations.extend(
                f"{rel_path}:{node.lineno}: imports {_CLASS_NAME} as "
                f"{alias.asname!r} -- construct it under its own name so "
                "the construction-site audit can see the site"
                for alias in node.names
                if alias.name == _CLASS_NAME and alias.asname is not None
            )
        elif (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Name)
            and node.value.id == _CLASS_NAME
        ):
            violations.append(
                f"{rel_path}:{node.lineno}: rebinds {_CLASS_NAME} to another "
                "name -- construct it under its own name so the "
                "construction-site audit can see the site"
            )
    return violations


def discover_production_context_sites() -> dict[str, DiscoveredSite]:
    """Every production construction, keyed as the registry keys them.

    Uses the typing ratchet's own production walker rather than a second
    one -- it already prunes ``tests/``, ``docs/``, and every hidden
    directory (``.claude/worktrees`` above all: an unpruned walk crawls
    thousands of stale worktree files).
    """
    sites: dict[str, DiscoveredSite] = {}
    for rel_path, abs_path in iter_production_paths():
        with open(abs_path, encoding="utf-8") as handle:
            source = handle.read()
        if _CLASS_NAME not in source:
            continue
        for site in _sites_in_module(rel_path, source):
            sites[site.key] = site
    return sites


def discover_alias_bindings() -> list[str]:
    """Every alias binding of the class across production code."""
    violations: list[str] = []
    for rel_path, abs_path in iter_production_paths():
        with open(abs_path, encoding="utf-8") as handle:
            source = handle.read()
        if _CLASS_NAME not in source:
            continue
        violations.extend(_alias_bindings(rel_path, source))
    return violations


def registry_violations(
    registry: Mapping[str, ContextSite],
    discovered: Mapping[str, DiscoveredSite],
) -> list[str]:
    """Where the registry and the tree disagree, one message per fact.

    Accumulating so no clause can mask another: a change that both adds an
    unregistered site AND drops a collaborator elsewhere reports both.
    """
    violations: list[str] = []
    for key in sorted(set(discovered) - set(registry)):
        site = discovered[key]
        violations.append(
            f"UNREGISTERED CratediggerContext construction at {key} "
            f"(line {site.lineno}). Add it to "
            "PRODUCTION_CONTEXT_CONSTRUCTIONS, declaring whether this "
            "context can reach a destructive slskd call and therefore "
            "needs download_ownership."
        )
    for key in sorted(set(registry) - set(discovered)):
        violations.append(
            f"STALE registry entry {key}: no CratediggerContext "
            "construction is there any more. Remove the entry."
        )
    for key in sorted(set(registry) & set(discovered)):
        expected = registry[key].wires_download_ownership
        actual = discovered[key].wires_download_ownership
        if expected != actual:
            violations.append(
                f"{key} (line {discovered[key].lineno}) "
                f"{'lost' if expected else 'gained'} its "
                "download_ownership= kwarg: the registry declares "
                f"wires_download_ownership={expected}, the source says "
                f"{actual}."
            )
    return violations


class TestProductionContextConstructionRegistryIsExact(unittest.TestCase):
    """The real registry, verified against the real production tree."""

    def test_registry_matches_the_tree(self) -> None:
        self.assertEqual(
            registry_violations(
                PRODUCTION_CONTEXT_CONSTRUCTIONS,
                discover_production_context_sites(),
            ),
            [],
        )

    def test_the_class_is_never_bound_to_another_name(self) -> None:
        self.assertEqual(discover_alias_bindings(), [])

    def test_every_registered_site_states_a_reason(self) -> None:
        """``why`` is the entry's whole value to the next author; an empty
        one turns the registry into a checkbox."""
        for key, site in sorted(PRODUCTION_CONTEXT_CONSTRUCTIONS.items()):
            with self.subTest(site=key):
                self.assertGreater(len(site.why.split()), 8, key)

    def test_the_owner_and_worker_contexts_are_registered_as_wiring_it(
        self,
    ) -> None:
        """A must-still-work floor under the registry itself.

        The audit above proves registry and tree AGREE; it cannot notice
        an author who flips a declaration to False and deletes the kwarg
        in the same commit. These three sites reach ``cancel_and_delete``
        and the answer for them is not a judgement call, so pin it here
        rather than leaving it to the diff.
        """
        for key in (
            "cratedigger.py::main",
            "cratedigger.py::build_phase1_context",
            "lib/enqueue.py::prepare_find_download_context",
        ):
            with self.subTest(site=key):
                self.assertTrue(
                    PRODUCTION_CONTEXT_CONSTRUCTIONS[
                        key].wires_download_ownership)


class TestRegistryCheckerTripsOnViolations(unittest.TestCase):
    """Known-bad self-tests: one named world per clause, asserting that
    clause's own message. Every clause is reachable from a real edit --
    add a construction, delete one, or drop the kwarg."""

    REGISTERED: ClassVar[dict[str, ContextSite]] = {
        "lib/thing.py::build": ContextSite(True, "wires it"),
    }

    def _discovered(self, **overrides: DiscoveredSite) -> dict[str, DiscoveredSite]:
        base = {
            "lib/thing.py::build": DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=True,
            ),
        }
        base.update(overrides)
        return base

    def test_unregistered_site_clause(self) -> None:
        discovered = self._discovered(**{
            "lib/other.py::make": DiscoveredSite(
                key="lib/other.py::make",
                lineno=42,
                wires_download_ownership=False,
            ),
        })

        violations = registry_violations(self.REGISTERED, discovered)

        self.assertEqual(len(violations), 1, violations)
        self.assertIn(
            "UNREGISTERED CratediggerContext construction at "
            "lib/other.py::make (line 42).",
            violations[0])

    def test_stale_registry_entry_clause(self) -> None:
        violations = registry_violations(self.REGISTERED, {})

        self.assertEqual(
            violations,
            [(
                "STALE registry entry lib/thing.py::build: no "
                "CratediggerContext construction is there any more. "
                "Remove the entry."
            )],
        )

    def test_lost_kwarg_clause(self) -> None:
        discovered = self._discovered(**{
            "lib/thing.py::build": DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=False,
            ),
        })

        violations = registry_violations(self.REGISTERED, discovered)

        self.assertEqual(
            violations,
            [(
                "lib/thing.py::build (line 10) lost its download_ownership= "
                "kwarg: the registry declares wires_download_ownership=True, "
                "the source says False."
            )],
        )

    def test_gained_kwarg_clause(self) -> None:
        registered = {"lib/thing.py::build": ContextSite(False, "cannot")}

        violations = registry_violations(registered, self._discovered())

        self.assertEqual(
            violations,
            [(
                "lib/thing.py::build (line 10) gained its download_ownership= "
                "kwarg: the registry declares wires_download_ownership=False, "
                "the source says True."
            )],
        )

    def test_every_clause_reports_when_several_are_violated(self) -> None:
        """Accumulating, not short-circuiting: one edit can trip several."""
        discovered = self._discovered(**{
            "lib/thing.py::build": DiscoveredSite(
                key="lib/thing.py::build",
                lineno=10,
                wires_download_ownership=False,
            ),
            "lib/other.py::make": DiscoveredSite(
                key="lib/other.py::make",
                lineno=42,
                wires_download_ownership=False,
            ),
        })
        registered = dict(self.REGISTERED)
        registered["lib/gone.py::old"] = ContextSite(True, "removed")

        violations = registry_violations(registered, discovered)

        self.assertEqual(len(violations), 3, violations)
        self.assertTrue(violations[0].startswith("UNREGISTERED"))
        self.assertTrue(violations[1].startswith("STALE"))
        self.assertIn("lost its download_ownership=", violations[2])

    def test_conforming_registry_reports_nothing(self) -> None:
        """Must-still-work: an exact registry raises no violation."""
        self.assertEqual(
            registry_violations(self.REGISTERED, self._discovered()), [])


class TestSiteDiscoveryGrammar(unittest.TestCase):
    """The bounded parse itself, on sources this test owns."""

    def test_both_recognised_spellings_are_found_with_their_scope(self) -> None:
        source = (
            "class Outer:\n"
            "    def build(self):\n"
            "        return CratediggerContext(cfg=1, download_ownership=w)\n"
            "\n"
            "def helper():\n"
            "    return context.CratediggerContext(cfg=1)\n"
            "\n"
            "TOP = CratediggerContext(cfg=1)\n"
        )

        sites = {s.key: s for s in _sites_in_module("lib/x.py", source)}

        self.assertEqual(
            set(sites),
            {
                "lib/x.py::Outer.build",
                "lib/x.py::helper",
                "lib/x.py::<module>",
            },
        )
        self.assertTrue(sites["lib/x.py::Outer.build"].wires_download_ownership)
        self.assertFalse(sites["lib/x.py::helper"].wires_download_ownership)

    def test_the_class_definition_itself_is_not_a_construction(self) -> None:
        source = (
            "class CratediggerContext:\n"
            "    pass\n"
        )
        self.assertEqual(_sites_in_module("lib/context.py", source), [])

    def test_a_nested_construction_keeps_its_full_scope_chain(self) -> None:
        source = (
            "def outer():\n"
            "    def inner():\n"
            "        return CratediggerContext(cfg=1)\n"
            "    return inner\n"
        )

        sites = _sites_in_module("lib/x.py", source)

        self.assertEqual([s.key for s in sites], ["lib/x.py::outer.inner"])

    def test_aliasing_the_class_is_a_violation(self) -> None:
        import_alias = (
            "from lib.context import CratediggerContext as Ctx\n"
        )
        rebind = (
            "from lib.context import CratediggerContext\n"
            "Ctx = CratediggerContext\n"
        )

        self.assertEqual(
            _alias_bindings("lib/x.py", import_alias),
            [(
                "lib/x.py:1: imports CratediggerContext as 'Ctx' -- "
                "construct it under its own name so the construction-site "
                "audit can see the site"
            )],
        )
        self.assertEqual(
            _alias_bindings("lib/x.py", rebind),
            [(
                "lib/x.py:2: rebinds CratediggerContext to another name -- "
                "construct it under its own name so the construction-site "
                "audit can see the site"
            )],
        )

    def test_a_plain_import_is_not_an_alias(self) -> None:
        """Must-still-work: the ordinary import every site uses."""
        self.assertEqual(
            _alias_bindings(
                "lib/x.py",
                "from lib.context import CratediggerContext\n"),
            [])


if __name__ == "__main__":
    unittest.main()
